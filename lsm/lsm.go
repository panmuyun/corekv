package lsm

import (
	"github.com/panmuyu/corekv/utils"
)

// LSM _
type LSM struct {
	memTable   *memTable     // 指向MemTable的指针
	immutables []*memTable   // 指针切片。存储了一组不可变的MemTable。当MemTable满时，它会被转换为Immutable MemTable并添加到这个队列中。
	levels     *levelManager //指向层级管理器的指针。层级管理器负责管理 LSM 树中的不同层级的 SSTable（Sorted String Table）文件。
	option     *Options      //指向配置选项的指针。这个配置选项包含了 LSM 树运行时的各种参数设置。
	closer     *utils.Closer //用于资源回收的信号控制。utils.Closer 可能是一个用于管理并发关闭操作的工具。
	maxMemFID  uint32        //最大内存文件ID，可能用于标识内存表或相关文件。
}

// Options _
type Options struct {
	WorkDir      string //工作目录，即 LSM 树数据文件的存储位置。
	MemTableSize int64  //内存表的最大大小（以字节为单位）。当内存表达到这个大小时，需要将其转换为不可变内存表或 SSTable。
	SSTableMaxSz int64  //每个 SSTable 文件的最大大小（以字节为单位）。当某个层级的 SSTable 文件达到这个大小时，可能需要进行合并操作。
	// BlockSize is the size of each block inside SSTable in bytes.
	BlockSize int //每个 SSTable 块的大小（以字节为单位）。
	// BloomFalsePositive is the false positive probabiltiy of bloom filter.
	BloomFalsePositive float64 //布隆过滤器的假阳性概率。

	// compact，控制LSM-tree的压缩过程和一些高级配置参数
	NumCompactors       int   //指定用于执行压缩任务的并发压缩器（compactor）的数量。
	BaseLevelSize       int64 //指定了基础层（通常是第0层之外的第一层）的预估大小，一般被指定为10兆字节（MB）
	LevelSizeMultiplier int   // 决定level之间期望的size比例。例如，如果该值为10，那么第1层的SSTable文件大小将是第0层的10倍，依次类推。
	TableSizeMultiplier int   //用于控制SSTable文件大小的增量倍数。这意味着每个SSTable文件的大小可以是上一个SSTable文件大小的若干倍。
	BaseTableSize       int64 //定义了SSTable文件的基本大小。这是各级别中最小的SSTable文件的大小。
	NumLevelZeroTables  int   //指定了第0层中可以存在的SSTable文件的最大数量。
	MaxLevelNum         int   //表示LSM树结构中最大的层级数量。

	DiscardStatsCh *chan map[uint32]int64 //是一个指向通道的指针，该通道用于传递丢弃统计信息。
}

// Close  _
func (lsm *LSM) Close() error {
	// 等待全部合并过程的结束
	// 等待全部api调用过程结束
	lsm.closer.Close()
	// TODO 需要加锁保证并发安全
	if lsm.memTable != nil {
		if err := lsm.memTable.close(); err != nil {
			return err
		}
	}
	for i := range lsm.immutables {
		if err := lsm.immutables[i].close(); err != nil {
			return err
		}
	}
	if err := lsm.levels.close(); err != nil {
		return err
	}
	return nil
}

// NewLSM _
func NewLSM(opt *Options) *LSM {
	lsm := &LSM{option: opt}
	// 初始化levelManager
	lsm.levels = lsm.initLevelManager(opt)
	// 启动DB恢复过程加载wal，如果没有恢复内容则创建新的内存表
	lsm.memTable, lsm.immutables = lsm.recovery()
	// 初始化closer 用于资源回收的信号控制
	lsm.closer = utils.NewCloser()
	return lsm
}

// StartCompacter _
func (lsm *LSM) StartCompacter() {
	n := lsm.option.NumCompactors
	lsm.closer.Add(n)
	for i := 0; i < n; i++ {
		go lsm.levels.runCompacter(i)
	}
}

// Set _
func (lsm *LSM) Set(entry *utils.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}
	// 优雅关闭
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	// 检查当前memtable是否写满，是的话创建新的memtable,并将当前内存表写到immutables中
	// 否则写入当前memtable中
	if int64(lsm.memTable.wal.Size())+
		int64(utils.EstimateWalCodecSize(entry)) > lsm.option.MemTableSize {
		lsm.Rotate()
	}

	if err = lsm.memTable.set(entry); err != nil {
		return err
	}
	// 检查是否存在immutable需要刷盘，
	for _, immutable := range lsm.immutables {
		if err = lsm.levels.flush(immutable); err != nil {
			return err
		}
		// TODO 这里问题很大，应该是用引用计数的方式回收
		err = immutable.close()
		utils.Panic(err)
	}
	if len(lsm.immutables) != 0 {
		// TODO 将lsm的immutables队列置空，这里可以优化一下节省内存空间，还可以限制一下immut table的大小为固定值
		lsm.immutables = make([]*memTable, 0)
	}
	return err
}

// Get _
func (lsm *LSM) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	lsm.closer.Add(1)
	defer lsm.closer.Done()
	var (
		entry *utils.Entry
		err   error
	)
	// 从内存表中查询,先查活跃表，在查不变表
	if entry, err = lsm.memTable.Get(key); entry != nil && entry.Value != nil {
		return entry, err
	}

	for i := len(lsm.immutables) - 1; i >= 0; i-- {
		if entry, err = lsm.immutables[i].Get(key); entry != nil && entry.Value != nil {
			return entry, err
		}
	}
	// 从level manger查询
	return lsm.levels.Get(key)
}

func (lsm *LSM) MemSize() int64 {
	return lsm.memTable.Size()
}

func (lsm *LSM) MemTableIsNil() bool {
	return lsm.memTable == nil
}

func (lsm *LSM) GetSkipListFromMemTable() *utils.Skiplist {
	return lsm.memTable.sl
}

// 把lsm.memTable追加到lsm.immutables中，并将lsm.memTable设置为新建的memTable对象
func (lsm *LSM) Rotate() {
	lsm.immutables = append(lsm.immutables, lsm.memTable)
	lsm.memTable = lsm.NewMemtable()
}
