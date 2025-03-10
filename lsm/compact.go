// Copyright 2021 panmuyun Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panmuyun/corekv/pb"
	"github.com/panmuyun/corekv/utils"
)

// 归并优先级
type compactionPriority struct {
	level        int      //表示要压缩的层级号
	score        float64  //表示当前压缩任务的得分
	adjusted     float64  //表示调整后的得分，可能是进一步优化或调整后的结果
	dropPrefixes [][]byte //包含要丢弃的键前缀的切片，用于在压缩过程中忽略某些特定的键
	t            targets  //表示压缩的目标信息
}

// 归并目标
type targets struct {
	baseLevel int     //表示归并操作的基础层级。
	targetSz  []int64 //存储了目标文件的大小。这些文件是在归并操作后生成的文件
	fileSz    []int64 //存储了当前层级中所有文件的大小。这个切片可能被用来判断哪些文件需要被归并。
}
type compactDef struct {
	compactorId int //进行该压缩任务的协程ID
	t           targets
	p           compactionPriority //表示归并操作的优先级。这个字段可能被用来决定不同归并任务的执行顺序
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*table //top可能代表了当前层级的文件
	bot []*table //bot可能代表了下一个层级的文件

	thisRange keyRange   //表示当前层级的键范围
	nextRange keyRange   //下一个层级的键范围
	splits    []keyRange //表示在归并过程中需要分裂的键范围。当目标文件的大小超过一定的限制时，可能需要将一个键范围的数据分裂成多个文件

	thisSize int64 //表示当前归并操作所涉及的文件的总大小

	dropPrefixes [][]byte //存储了在归并过程中需要丢弃的键的前缀
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.nextLevel.RUnlock()
	cd.thisLevel.RUnlock()
}

// runCompacter 启动一个compacter
func (lm *levelManager) runCompacter(id int) {
	defer lm.lsm.closer.Done()
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond) //创建一个定时器，随机延迟一段时间（0到999毫秒之间）。
	select {                                                                          //用于等待多个通道操作，这里监听两个通道
	case <-randomDelay.C: //等待随机延迟定时器触发
	case <-lm.lsm.closer.CloseSignal: //监听关闭信号通道，如果接收到关闭信号，则停止定时器并返回，结束函数执行
		randomDelay.Stop()
		return
	}
	//TODO 这个值有待验证
	ticker := time.NewTicker(50000 * time.Millisecond) //创建一个每隔50000毫秒（即50秒）触发一次的定时器，用于定期执行压缩操作
	defer ticker.Stop()
	for {
		select {
		// Can add a done channel or other stuff.
		case <-ticker.C: //当定时器触发时，调用lm.runOnce(id)方法执行一次压缩操作
			lm.runOnce(id)
		case <-lm.lsm.closer.CloseSignal: //如果接收到关闭信号，则立即返回，结束循环和函数
			return
		}
	}
}

// runOnce：根据优先级列表选择要执行的压缩操作，并尝试运行这些操作
func (lm *levelManager) runOnce(id int) bool {
	prios := lm.pickCompactLevels() //获取一个压缩优先级列表 prios。这个列表包含了需要进行压缩的不同层级（level）的信息
	if id == 0 {
		// 0号协程 优先处理 l0 层的压缩任务
		prios = moveL0toFront(prios)
	}
	for _, p := range prios {
		if id == 0 && p.level == 0 {
			// 对于l0，无论得分多少都要运行
		} else if p.adjusted < 1.0 {
			// 对于其他level，如果压缩得分adjusted小于1，则不执行
			break
		}
		if lm.run(id, p) {
			return true
		}
	}
	return false
}

// 将优先级列表中的 l0 层（通常是第 0 层）移动到列表的最前面。
func moveL0toFront(prios []compactionPriority) []compactionPriority {
	idx := -1
	for i, p := range prios {
		if p.level == 0 {
			idx = i
			break
		}
	}
	// If idx == -1, we didn't find L0.
	// If idx == 0, then we don't need to do anything. L0 is already at the front.
	if idx > 0 {
		out := append([]compactionPriority{}, prios[idx])
		out = append(out, prios[:idx]...)
		out = append(out, prios[idx+1:]...)
		return out
	}
	return prios
}

// run 执行一个优先级指定的合并任务
func (lm *levelManager) run(id int, p compactionPriority) bool {
	err := lm.doCompact(id, p)
	switch err {
	case nil:
		return true
	case utils.ErrFillTables:
		// 什么也不做，此时合并过程被忽略
	default:
		log.Printf("[taskID:%d] While running doCompact: %v\n ", id, err)
	}
	return false
}

// doCompact 选择level的某些表合并到目标level
func (lm *levelManager) doCompact(id int, p compactionPriority) error {
	l := p.level
	utils.CondPanic(l >= lm.opt.MaxLevelNum, errors.New("[doCompact] Sanity check. l >= lm.opt.MaxLevelNum")) // Sanity check.
	if p.t.baseLevel == 0 {                                                                                   //如果 baseLevel 为 0，说明当前压缩任务的 baseLevel 尚未被设置
		p.t = lm.levelTargets()
	}
	// 创建真正的压缩计划
	cd := compactDef{
		compactorId:  id,
		p:            p,
		t:            p.t,
		thisLevel:    lm.levels[l],
		dropPrefixes: p.dropPrefixes,
	}

	// 如果是第0层 对齐单独填充处理
	if l == 0 {
		cd.nextLevel = lm.levels[p.t.baseLevel]
		if !lm.fillTablesL0(&cd) {
			return utils.ErrFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel //将cd.nextLevel设置为当前层thisLevel
		// 如果不是最后一层，则压缩到下一层即可
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = lm.levels[l+1]
		}
		if !lm.fillTables(&cd) {
			return utils.ErrFillTables
		}
	}
	// 完成合并后 从合并状态中删除
	defer lm.compactState.delete(cd) // Remove the ranges from compaction status.

	// 执行合并计划
	if err := lm.runCompactDef(id, l, cd); err != nil {
		// This compaction couldn't be done successfully.
		log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)
	return nil
}

// pickCompactLevel 选择合适的level执行合并，返回判断的优先级
func (lm *levelManager) pickCompactLevels() (prios []compactionPriority) {
	t := lm.levelTargets()
	addPriority := func(level int, score float64) {
		pri := compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			t:        t,
		}
		prios = append(prios, pri)
	}

	// 根据l0表的table数量来对压缩提权
	addPriority(0, float64(lm.levels[0].numTables())/float64(lm.opt.NumLevelZeroTables))

	// 非l0 层都根据大小计算优先级
	for i := 1; i < len(lm.levels); i++ {
		// 处于压缩状态的sst 不能计算在内
		delSize := lm.compactState.delSize(i)
		l := lm.levels[i]
		sz := l.getTotalSize() - delSize
		// score的计算是 扣除正在合并的表后的尺寸与目标sz的比值
		addPriority(i, float64(sz)/float64(t.targetSz[i]))
	}
	utils.CondPanic(len(prios) != len(lm.levels), errors.New("[pickCompactLevels] len(prios) != len(lm.levels)"))

	// 调整得分
	var prevLevel int
	for level := t.baseLevel; level < len(lm.levels); level++ {
		if prios[prevLevel].adjusted >= 1 {
			// 避免过大的得分
			const minScore = 0.01
			if prios[level].score >= minScore {
				prios[prevLevel].adjusted /= prios[level].adjusted
			} else {
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	// 仅选择得分大于1的压缩内容，并且允许l0到l0的特殊压缩，为了提升查询性能允许l0层独自压缩
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	// 按优先级排序
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}
func (lm *levelManager) lastLevel() *levelHandler {
	return lm.levels[len(lm.levels)-1]
}

// levelTargets：根据一些配置和层级信息来计算每个层级的目标大小和文件大小，并确定一个基础层级（baseLevel）
func (lm *levelManager) levelTargets() targets {
	adjust := func(sz int64) int64 { //定义调整大小的函数
		if sz < lm.opt.BaseLevelSize {
			return lm.opt.BaseLevelSize
		}
		return sz
	}

	// 初始化默认都是最大层级
	t := targets{
		targetSz: make([]int64, len(lm.levels)),
		fileSz:   make([]int64, len(lm.levels)),
	}
	//计算每个level的总大小的期望size,并将其存储在targets.targetSZ中
	dbSize := lm.lastLevel().getTotalSize()   //最后一层中所有表的总大小
	for i := len(lm.levels) - 1; i > 0; i-- { // 从最后一个level开始计算
		leveTargetSize := adjust(dbSize)
		t.targetSz[i] = leveTargetSize
		// 如果当前的level没有达到合并的要求
		if t.baseLevel == 0 && leveTargetSize <= lm.opt.BaseLevelSize {
			t.baseLevel = i
		}
		dbSize /= int64(lm.opt.LevelSizeMultiplier) //每个level的期望size之间相差一个数量级。为上一层计算目标大小
	}
	//计算每个level中一个SST文件的期望大小,并将其存储在targets.fileSZ中
	tsz := lm.opt.BaseTableSize
	for i := 0; i < len(lm.levels); i++ {
		if i == 0 {
			// l0选择memtable的size作为文件的尺寸
			t.fileSz[i] = lm.opt.MemTableSize
		} else if i <= t.baseLevel {
			t.fileSz[i] = tsz
		} else {
			tsz *= int64(lm.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}

	// 找到最后一个空level作为目标level实现跨level归并，减少写放大
	for i := t.baseLevel + 1; i < len(lm.levels)-1; i++ {
		if lm.levels[i].getTotalSize() > 0 { //中间几层必须为空
			break
		}
		t.baseLevel = i
	}

	// 如果存在断层，则目标level++
	b := t.baseLevel
	lvl := lm.levels
	if b < len(lvl)-1 && lvl[b].getTotalSize() == 0 && lvl[b+1].getTotalSize() < t.targetSz[b+1] {
		t.baseLevel++
	}
	return t
}

type thisAndNextLevelRLocked struct{}

func (lm *levelManager) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	tables := make([]*table, cd.thisLevel.numTables())
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}
	// We're doing a maxLevel to maxLevel compaction. Pick tables based on the stale data size.
	if cd.thisLevel.isLastLevel() {
		return lm.fillMaxLevelTables(tables, cd)
	}
	// We pick tables, so we compact older tables first. This is similar to
	// kOldestLargestSeqFirst in RocksDB.
	lm.sortByHeuristic(tables, cd)

	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// 如果被压缩过了，则什么都不需要做
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}
		cd.top = []*table{t}
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)

		cd.bot = make([]*table, right-left)
		copy(cd.bot, cd.nextLevel.tables[left:right])

		if len(cd.bot) == 0 {
			cd.bot = []*table{}
			cd.nextRange = cd.thisRange
			if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(cd.bot...)

		if lm.compactState.overlapsWith(cd.nextLevel.levelNum, cd.nextRange) {
			continue
		}
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true
	}
	return false
}

// compact older tables first.按表的最大版本号（MaxVersion）进行升序排序。
func (lm *levelManager) sortByHeuristic(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	// Sort tables by max version. This is what RocksDB does.
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].ss.Indexs().MaxVersion < tables[j].ss.Indexs().MaxVersion
	})
}
func (lm *levelManager) runCompactDef(id, l int, cd compactDef) (err error) {
	if len(cd.t.fileSz) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	utils.CondPanic(len(cd.splits) != 0, errors.New("len(cd.splits) != 0"))
	if thisLevel == nextLevel {
		// l0 to l0 和 lmax to lmax 不做特殊处理
	} else {
		lm.addSplits(&cd)
	}
	// 追加一个空的
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	newTables, decr, err := lm.compactBuildTables(l, cd)
	if err != nil {
		return err
	}
	defer func() {
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	changeSet := buildChangeSet(&cd, newTables)

	// 删除之前先更新manifest文件
	if err := lm.manifestFile.AddChanges(changeSet.Changes); err != nil {
		return err
	}

	if err := nextLevel.replaceTables(cd.bot, newTables); err != nil {
		return err
	}
	defer decrRefs(cd.top)
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	from := append(tablesToString(cd.top), tablesToString(cd.bot)...)
	to := tablesToString(newTables)
	if dur := time.Since(timeStart); dur > 2*time.Second { //如果压缩操作的持续时间超过2秒，则输出一条日志。
		var expensive string
		if dur > time.Second { //如果持续时间超过1秒，还会在日志中标记为 [E]，表示这是一个昂贵的操作
			expensive = " [E]"
		}
		fmt.Printf("[%d]%s LOG Compact %d->%d (%d, %d -> %d tables with %d splits)."+
			" [%s] -> [%s], took %v\n",
			id, expensive, thisLevel.levelNum, nextLevel.levelNum, len(cd.top), len(cd.bot),
			len(newTables), len(cd.splits), strings.Join(from, " "), strings.Join(to, " "),
			dur.Round(time.Millisecond))
	}
	return nil
}

// tablesToString
func tablesToString(tables []*table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.fid))
	}
	res = append(res, ".")
	return res
}

// buildChangeSet _
func buildChangeSet(cd *compactDef, newTables []*table) pb.ManifestChangeSet {
	changes := []*pb.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes, newCreateChange(table.fid, cd.nextLevel.levelNum))
	}
	for _, table := range cd.top {
		changes = append(changes, newDeleteChange(table.fid))
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.fid))
	}
	return pb.ManifestChangeSet{Changes: changes}
}

func newDeleteChange(id uint64) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id: id,
		Op: pb.ManifestChange_DELETE,
	}
}

// newCreateChange
func newCreateChange(id uint64, level int) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:    id,
		Op:    pb.ManifestChange_CREATE,
		Level: uint32(level),
	}
}

// compactBuildTables 合并两个层的sst文件
func (lm *levelManager) compactBuildTables(lev int, cd compactDef) ([]*table, func() error, error) {

	topTables := cd.top
	botTables := cd.bot
	iterOpt := &utils.Options{
		IsAsc: true,
	}
	//numTables := int64(len(topTables) + len(botTables))
	newIterator := func() []utils.Iterator { // 创建一个新的迭代器函数，用于遍历所有涉及的表
		// Create iterators across all the tables involved first.
		var iters []utils.Iterator
		switch {
		case lev == 0: // 如果层级为0，对顶层表进行反向迭代
			iters = append(iters, iteratorsReversed(topTables, iterOpt)...)
		case len(topTables) > 0: // 如果层级不为0且顶层表存在，只对第一个顶层表创建迭代器
			iters = []utils.Iterator{topTables[0].NewIterator(iterOpt)}
		}
		return append(iters, NewConcatIterator(botTables, iterOpt)) //添加bottom表的合并迭代器
	}

	// 开始并行执行压缩过程
	res := make(chan *table, 3)                               // 创建一个通道用于接收压缩生成的新表
	inflightBuilders := utils.NewThrottle(8 + len(cd.splits)) // 创建一个节流器，用于控制并发压缩的数量
	for _, kr := range cd.splits {
		// Initiate Do here so we can register the goroutines for buildTables too.
		// 在创建协程之前调用Do方法，以确保可以注册子压缩的协程
		if err := inflightBuilders.Do(); err != nil {
			return nil, nil, fmt.Errorf("cannot start subcompaction: %+v", err)
		}
		// 开启一个协程去处理子压缩
		go func(kr keyRange) {
			defer inflightBuilders.Done(nil)
			it := NewMergeIterator(newIterator(), false) // 创建一个合并迭代器用于迭代新生成的数据
			defer it.Close()
			lm.subcompact(it, kr, cd, inflightBuilders, res) // 调用子压缩函数进行压缩操作
		}(kr)
	}

	// mapreduce的方式收集table的句柄
	var newTables []*table
	var wg sync.WaitGroup // 创建一个等待组用于等待所有协程完成
	wg.Add(1)
	go func() { // 开启一个协程收集压缩生成的新表
		defer wg.Done()
		for t := range res { // 从通道接收新的表，并添加到新表切片中
			newTables = append(newTables, t)
		}
	}()

	// 在这里等待所有的压缩过程完成
	err := inflightBuilders.Finish()
	// channel 资源回收
	close(res)
	// 等待所有的builder刷到磁盘
	wg.Wait()

	if err == nil {
		// 同步刷盘，保证数据一定落盘
		err = utils.SyncDir(lm.opt.WorkDir)
	}

	if err != nil {
		// 如果出现错误，则删除索引新创建的文件
		_ = decrRefs(newTables)
		return nil, nil, fmt.Errorf("while running compactions for: %+v, %v", cd, err)
	}

	sort.Slice(newTables, func(i, j int) bool { // 对新创建的表按最大键值进行排序
		return utils.CompareKeys(newTables[i].ss.MaxKey(), newTables[j].ss.MaxKey()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

// 并行的运行子压缩情况
func (lm *levelManager) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// Let's say we have 10 tables in cd.bot and min width = 3. Then, we'll pick
	// 0, 1, 2 (pick), 3, 4, 5 (pick), 6, 7, 8 (pick), 9 (pick, because last table).
	// This gives us 4 picks for 10 tables.
	// In an edge case, 142 tables in bottom led to 48 splits. That's too many splits, because it
	// then uses up a lot of memory for table builder.
	// We should keep it so we have at max 5 splits.
	width := int(math.Ceil(float64(len(cd.bot)) / 5.0)) // 计算每个子数组的宽度，确保最多有5个分割点
	if width < 3 {                                      //控制子数组宽度 >= 3
		width = 3
	}
	skr := cd.thisRange
	skr.extend(cd.nextRange)

	addRange := func(right []byte) { //定义一个函数用于添加新的分割点
		skr.right = utils.Copy(right)
		cd.splits = append(cd.splits, skr)
		skr.left = skr.right
	}

	for i, t := range cd.bot {
		// last entry in bottom table.
		if i == len(cd.bot)-1 { // 如果是最后一个元素，则添加一个空的右区间作为分割点，并结束循环
			addRange([]byte{})
			return
		}
		if i%width == width-1 { // 如果当前索引是width-1的倍数，则计算右区间并添加新的分割点
			// 设置最大值为右区间
			right := utils.KeyWithTs(utils.ParseKey(t.ss.MaxKey()), math.MaxUint64)
			addRange(right)
		}
	}
}

// sortByStaleData 对表中陈旧数据的数量对sst文件进行排序
func (lm *levelManager) sortByStaleDataSize(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	// TODO 统计一个 sst文件中陈旧数据的数量，涉及对存储格式的修改
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})
}

// max level 和 max level 的压缩：用于处理从最高层级（max level）到最高层级的合并操作。合并操作的目的是清理过时的数据，优化存储结构
func (lm *levelManager) fillMaxLevelTables(tables []*table, cd *compactDef) bool {
	sortedTables := make([]*table, len(tables))
	copy(sortedTables, tables)
	lm.sortByStaleDataSize(sortedTables, cd) //根据过时数据（stale data）的大小对sortedTables进行排序

	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 { //没有需要合并的数据
		// This is a maxLevel to maxLevel compaction and we don't have any stale data.
		return false
	}
	cd.bot = []*table{}
	collectBotTables := func(t *table, needSz int64) { //定义一个内部函数collectBotTables，用于收集需要合并的table
		totalSize := t.Size()

		j := sort.Search(len(tables), func(i int) bool { //使用sort.Search找到tables中第一个MinKey不小于t.MinKey的table的索引
			return utils.CompareKeys(tables[i].ss.MinKey(), t.ss.MinKey()) >= 0
		})
		utils.CondPanic(tables[j].fid != t.fid, errors.New("tables[j].ID() != t.ID()")) // 确认找到的table与当前table是同一个文件ID（fid）
		j++
		// Collect tables until we reach the the required size.
		for j < len(tables) { // 循环收集table，直到总大小达到needSz或遍历完所有table
			newT := tables[j]
			totalSize += newT.Size()

			if totalSize >= needSz {
				break
			}
			cd.bot = append(cd.bot, newT)
			cd.nextRange.extend(getKeyRange(newT))
			j++
		}
	}
	now := time.Now()
	for _, t := range sortedTables {
		if now.Sub(*t.GetCreatedAt()) < time.Hour { // 如果table是最近一小时内创建的，跳过这次合并
			// Just created it an hour ago. Don't pick for compaction.
			continue
		}
		// If the stale data size is less than 10 MB, it might not be worth
		// rewriting the table. Skip it.
		if t.StaleDataSize() < 10<<20 { // 如果table中过时数据的大小小于10MB，合并这个table可能不值得，也跳过
			continue
		}

		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// Set the next range as the same as the current range. If we don't do
		// this, we won't be able to run more than one max level compactions.
		cd.nextRange = cd.thisRange
		// If we're already compacting this range, don't do anything.
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) { // 如果当前compactState中已经存在这个范围的合并操作，跳过
			continue
		}

		// Found a valid table!
		cd.top = []*table{t} // 找到了一个有效的table，设置cd.top为包含这个table的切片

		needFileSz := cd.t.fileSz[cd.thisLevel.levelNum]

		if t.Size() >= needFileSz { // 如果当前table的大小已经大于或等于所需的文件大小，直接终止并返回true
			break
		}
		// TableSize is less than what we want. Collect more tables for compaction.
		// If the level has multiple small tables, we collect all of them
		// together to form a bigger table.
		collectBotTables(t, needFileSz) // 如果table的大小小于所需的文件大小，就需要收集更多的table来形成一个更大的table
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			cd.bot = cd.bot[:0]
			cd.nextRange = keyRange{}
			continue
		}
		return true
	}
	if len(cd.top) == 0 { // 如果没有找到需要合并的table，返回false
		return false
	}

	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) // 尝试将当前的合并定义添加到compactState中，并返回结果
}

// fillTablesL0 先尝试从l0 到lbase的压缩，如果失败则对l0自己压缩
func (lm *levelManager) fillTablesL0(cd *compactDef) bool {
	if ok := lm.fillTablesL0ToLbase(cd); ok {
		return true
	}
	return lm.fillTablesL0ToL0(cd)
}

func (lm *levelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		utils.Panic(errors.New("base level can’t be zero"))
	}
	// 如果优先级低于1 则不执行
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		// Do not compact to Lbase if adjusted score is less than 1.0.
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}

	var out []*table
	var kr keyRange
	// cd.top[0] 是最老的文件，从最老的文件开始
	for _, t := range top {
		dkr := getKeyRange(t)
		if kr.overlapsWith(dkr) {
			out = append(out, t)
			kr.extend(dkr)
		} else {
			// 如果有任何一个不重合的区间存在则直接终止
			break
		}
	}
	// 获取目标range list 的全局 range 对象
	cd.thisRange = getKeyRange(out...)
	cd.top = out

	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])

	if len(cd.bot) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}
	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0ToL0 l0到l0压缩
func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorId != 0 {
		// 只要0号压缩处理器可以执行，避免l0tol0的资源竞争
		return false
	}

	cd.nextLevel = lm.levels[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	//  TODO 这里是否会导致死锁？
	utils.CondPanic(cd.thisLevel.levelNum != 0, errors.New("cd.thisLevel.levelNum != 0"))
	utils.CondPanic(cd.nextLevel.levelNum != 0, errors.New("cd.nextLevel.levelNum != 0"))
	lm.levels[0].RLock()
	defer lm.levels[0].RUnlock()

	lm.compactState.Lock()
	defer lm.compactState.Unlock()

	top := cd.thisLevel.tables
	var out []*table
	now := time.Now()
	for _, t := range top {
		if t.Size() >= 2*cd.t.fileSz[0] {
			// 在L0 to L0 的压缩过程中，不要对过大的sst文件压缩，这会造成性能抖动
			continue
		}
		if now.Sub(*t.GetCreatedAt()) < 10*time.Second {
			// 如果sst的创建时间不足10s 也不要回收
			continue
		}
		// 如果当前的sst 已经在压缩状态 也应该忽略
		if _, beingCompacted := lm.compactState.tables[t.fid]; beingCompacted {
			continue
		}
		out = append(out, t)
	}

	if len(out) < 4 {
		// 满足条件的sst小于4个那就不压缩了
		return false
	}
	cd.thisRange = infRange
	cd.top = out

	// 在这个过程中避免任何l0到其他层的合并
	thisLevel := lm.compactState.levels[cd.thisLevel.levelNum]
	thisLevel.ranges = append(thisLevel.ranges, infRange)
	for _, t := range out {
		lm.compactState.tables[t.fid] = struct{}{}
	}

	//  l0 to l0的压缩最终都会压缩为一个文件，这大大减少了l0层文件数量，减少了读放大
	cd.t.fileSz[0] = math.MaxUint32
	return true
}

// getKeyRange 返回一组sst的区间合并后的最大与最小值
func getKeyRange(tables ...*table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}
	minKey := tables[0].ss.MinKey()
	maxKey := tables[0].ss.MaxKey()
	for i := 1; i < len(tables); i++ {
		if utils.CompareKeys(tables[i].ss.MinKey(), minKey) < 0 {
			minKey = tables[i].ss.MinKey()
		}
		if utils.CompareKeys(tables[i].ss.MaxKey(), maxKey) > 0 {
			maxKey = tables[i].ss.MaxKey()
		}
	}

	// We pick all the versions of the smallest and the biggest key. Note that version zero would
	// be the rightmost key, considering versions are default sorted in descending order.
	return keyRange{
		left:  utils.KeyWithTs(utils.ParseKey(minKey), math.MaxUint64),
		right: utils.KeyWithTs(utils.ParseKey(maxKey), 0),
	}
}

func iteratorsReversed(th []*table, opt *utils.Options) []utils.Iterator {
	out := make([]utils.Iterator, 0, len(th))
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator(opt))
	}
	return out
}
func (lm *levelManager) updateDiscardStats(discardStats map[uint32]int64) {
	select {
	case *lm.lsm.option.DiscardStatsCh <- discardStats:
	default:
	}
}

// 真正执行并行压缩的子压缩文件
func (lm *levelManager) subcompact(it utils.Iterator, kr keyRange, cd compactDef,
	inflightBuilders *utils.Throttle, res chan<- *table) {
	var lastKey []byte
	// 更新 discardStats
	discardStats := make(map[uint32]int64)
	defer func() {
		lm.updateDiscardStats(discardStats)
	}()
	// 更新统计信息的函数
	updateStats := func(e *utils.Entry) {
		if e.Meta&utils.BitValuePointer > 0 { // 检查 entry 是否包含值的指针
			var vp utils.ValuePtr
			vp.Decode(e.Value)                    // 解码值的指针
			discardStats[vp.Fid] += int64(vp.Len) // 更新 discardStats 中对应文件ID的数据丢弃量
		}
	}
	// 向 tableBuilder 添加键的函数
	addKeys := func(builder *tableBuilder) {
		var tableKr keyRange
		for ; it.Valid(); it.Next() { // 遍历迭代器中的所有条目
			key := it.Item().Entry().Key
			//version := utils.ParseTs(key)
			isExpired := IsDeletedOrExpired(it.Item().Entry()) // 检查该键是否已删除或过期
			if !utils.SameKey(key, lastKey) {
				// 如果迭代器返回的key大于当前key的范围就不用执行了
				if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					// 如果超过预估的sst文件大小，则直接结束
					break
				}
				// 把当前的key变为 lastKey
				lastKey = utils.SafeCopy(lastKey, key)
				//umVersions = 0
				// 如果左边界没有，则当前key给到左边界
				if len(tableKr.left) == 0 {
					tableKr.left = utils.SafeCopy(tableKr.left, key)
				}
				// 更新右边界
				tableKr.right = lastKey
			}
			// TODO 这里要区分值的指针
			// 判断是否是过期内容，是的话就删除
			switch {
			case isExpired:
				updateStats(it.Item().Entry())
				builder.AddStaleKey(it.Item().Entry())
			default:
				builder.AddKey(it.Item().Entry())
			}
		}
	} // End of function: addKeys

	//如果 key range left还存在 则seek到这里 说明遍历中途停止了
	if len(kr.left) > 0 {
		it.Seek(kr.left)
	} else {
		//如果左边界不存在，则将迭代器重置到开头
		it.Rewind()
	}
	for it.Valid() { // 遍历迭代器，直到右边界或迭代器结束
		key := it.Item().Entry().Key
		if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
			break
		}
		// 拼装table创建的参数
		// TODO 这里可能要大改，对open table的参数复制一份opt
		builder := newTableBuilerWithSSTSize(lm.opt, cd.t.fileSz[cd.nextLevel.levelNum])

		// This would do the iteration and add keys to builder.
		addKeys(builder)

		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		if builder.empty() {
			// Cleanup builder resources:
			builder.finish()
			builder.Close()
			continue
		}
		if err := inflightBuilders.Do(); err != nil { // 如果无法创建更多的 builder，则停止处理
			// Can't return from here, until I decrRef all the tables that I built so far.
			break
		}
		// 充分发挥 ssd的并行 写入特性
		// 使用 goroutine 来并行执行写入操作，以充分利用 SSD 的并行写入能力
		go func(builder *tableBuilder) {
			defer inflightBuilders.Done(nil)
			defer builder.Close()
			var tbl *table
			newFID := atomic.AddUint64(&lm.maxFID, 1) // compact的时候是没有memtable的，这里自增maxFID即可。
			// TODO 这里的sst文件需要根据level大小变化
			sstName := utils.FileNameSSTable(lm.opt.WorkDir, newFID)
			tbl = openTable(lm, sstName, builder)
			if tbl == nil {
				return
			}
			res <- tbl
		}(builder)
	}
}

// checkOverlap 检查是否与下一层存在重合
func (lm *levelManager) checkOverlap(tables []*table, lev int) bool {
	kr := getKeyRange(tables...)
	for i, lh := range lm.levels {
		if i < lev { // Skip upper levels.
			continue
		}
		lh.RLock()
		left, right := lh.overlappingTables(levelHandlerRLocked{}, kr)
		lh.RUnlock()
		if right-left > 0 {
			return true
		}
	}
	return false
}

// 判断是否过期 是可删除
func IsDeletedOrExpired(e *utils.Entry) bool {
	if e.Value == nil {
		return true
	}
	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}

// compactStatus
type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

func (lsm *LSM) newCompactStatus() *compactStatus {
	cs := &compactStatus{
		levels: make([]*levelCompactStatus, 0),
		tables: make(map[uint64]struct{}),
	}
	for i := 0; i < lsm.option.MaxLevelNum; i++ {
		cs.levels = append(cs.levels, &levelCompactStatus{})
	}
	return cs
}

func (cs *compactStatus) overlapsWith(level int, this keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(this)
}

func (cs *compactStatus) delSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	return cs.levels[l].delSize
}

// 从lcs.ranges中删除Lx层和Ly层的keyRange;从cs.tables中删除Lx层和Ly层的参与合并的table
func (cs *compactStatus) delete(cd compactDef) {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum

	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	thisLevel.delSize -= cd.thisSize
	found := thisLevel.remove(cd.thisRange)
	// The following check makes sense only if we're compacting more than one
	// table. In case of the max level, we might rewrite a single table to
	// remove stale data.
	if cd.thisLevel != cd.nextLevel && !cd.nextRange.isEmpty() {
		found = nextLevel.remove(cd.nextRange) && found
	}

	if !found {
		this := cd.thisRange
		next := cd.nextRange
		fmt.Printf("Looking for: %s in this level %d.\n", this, tl)
		fmt.Printf("This Level:\n%s\n", thisLevel.debug())
		fmt.Println()
		fmt.Printf("Looking for: %s in next level %d.\n", next, cd.nextLevel.levelNum)
		fmt.Printf("Next Level:\n%s\n", nextLevel.debug())
		log.Fatal("keyRange not found")
	}
	for _, t := range append(cd.top, cd.bot...) {
		_, ok := cs.tables[t.fid]
		utils.CondPanic(!ok, fmt.Errorf("cs.tables is nil"))
		delete(cs.tables, t.fid)
	}
}

// 向compactStatus中比较并添加压缩任务cd中的数据（如keyRange、tables）
func (cs *compactStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd compactDef) bool {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum
	utils.CondPanic(tl >= len(cs.levels), fmt.Errorf("Got level %d. Max levels: %d", tl, len(cs.levels)))
	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	if thisLevel.overlapsWith(cd.thisRange) {
		return false
	}
	if nextLevel.overlapsWith(cd.nextRange) {
		return false
	}
	// Check whether this level really needs compaction or not. Otherwise, we'll end up
	// running parallel compactions for the same level.
	// Update: We should not be checking size here. Compaction priority already did the size checks.
	// Here we should just be executing the wish of others.

	thisLevel.ranges = append(thisLevel.ranges, cd.thisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.nextRange)
	thisLevel.delSize += cd.thisSize
	for _, t := range append(cd.top, cd.bot...) {
		cs.tables[t.fid] = struct{}{}
	}
	return true
}

// levelCompactStatus
type levelCompactStatus struct {
	ranges  []keyRange //用于存储在该层级中存在的键范围（key ranges）
	delSize int64      //表示在该层级中被删除的数据的总大小。
}

// 检查levelCompactStatus.ranges中是否有与dst重叠的
func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}
func (lcs *levelCompactStatus) remove(dst keyRange) bool {
	final := lcs.ranges[:0]
	var found bool
	for _, r := range lcs.ranges {
		if !r.equals(dst) {
			final = append(final, r)
		} else {
			found = true
		}
	}
	lcs.ranges = final
	return found
}

func (lcs *levelCompactStatus) debug() string {
	var b bytes.Buffer
	for _, r := range lcs.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}

// keyRange
type keyRange struct {
	left  []byte
	right []byte
	inf   bool
	size  int64 // size is used for Key splits.
}

// 检查当前KeyRange对象是否为[0,0]
func (r keyRange) isEmpty() bool {
	return len(r.left) == 0 && len(r.right) == 0 && !r.inf
}

var infRange = keyRange{inf: true}

func (r keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", r.left, r.right, r.inf)
}

// 检查是否与dst相等
func (r keyRange) equals(dst keyRange) bool {
	return bytes.Equal(r.left, dst.left) &&
		bytes.Equal(r.right, dst.right) &&
		r.inf == dst.inf
}

// 将keyRange的值扩展到与kr相同
func (r *keyRange) extend(kr keyRange) {
	if kr.isEmpty() {
		return
	}
	if r.isEmpty() {
		*r = kr
	}
	if len(r.left) == 0 || utils.CompareKeys(kr.left, r.left) < 0 {
		r.left = kr.left
	}
	if len(r.right) == 0 || utils.CompareKeys(kr.right, r.right) > 0 {
		r.right = kr.right
	}
	if kr.inf {
		r.inf = true
	}
}

// 检查与dst是否有重叠区间
func (r keyRange) overlapsWith(dst keyRange) bool {
	// Empty keyRange always overlaps.
	if r.isEmpty() {
		return true
	}
	// Empty dst doesn't overlap with anything.
	if dst.isEmpty() {
		return false
	}
	if r.inf || dst.inf {
		return true
	}

	// [dst.left, dst.right] ... [r.left, r.right]
	// If my left is greater than dst right, we have no overlap.
	if utils.CompareKeys(r.left, dst.right) > 0 {
		return false
	}
	// [r.left, r.right] ... [dst.left, dst.right]
	// If my right is less than dst left, we have no overlap.
	if utils.CompareKeys(r.right, dst.left) < 0 {
		return false
	}
	// We have overlap.
	return true
}
