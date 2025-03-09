package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
	"unsafe"

	xxhash "github.com/cespare/xxhash/v2"
)

type Cache struct {
	mutex           sync.RWMutex
	windowbuf       *windowLRU
	mainbuf         *segmentedLRU
	filter          *BloomFilter
	counters        *cmSketch                // 记录访问计数
	sample          int32                    // 当前采样数量
	sampleThreshold int32                    // 采样阈值，到达阈值计数会减半
	data            map[uint64]*list.Element // 与windowbuf.data、mainbuf.data共用，是同一块内存空间
	hits            int32
	misses          int32
}

type Options struct {
	windowbufPct uint8
}

// NewCache size 指的是要缓存的数据个数
func NewCache(size int) *Cache {
	//定义 Window-LRU 部分缓存所占百分比，这里定义为1%
	const windowbufPct = 1
	//计算出来 window 部分的容量
	windowbufSz := (windowbufPct * size) / 100

	if windowbufSz < 1 {
		windowbufSz = 1
	}

	// 计算 segmented-LFU 部分的缓存容量
	mainbufSz := int(float64(size) * ((100 - windowbufPct) / 100.0))

	if mainbufSz < 1 {
		mainbufSz = 1
	}

	//LFU 分为两部分，stageOne 部分占比20%
	mainbufO := int(0.2 * float64(mainbufSz))

	if mainbufO < 1 {
		mainbufO = 1
	}

	data := make(map[uint64]*list.Element, size)

	return &Cache{
		windowbuf: newWindowLRU(windowbufSz, data),
		mainbuf:   newSLRU(data, mainbufO, mainbufSz-mainbufO),
		filter:    newFilter(size, 0.01), //布隆过滤器设置误差率为0.01
		counters:  newCmSketch(int64(size)),
		data:      data, //共用同一个 map 存储数据
	}

}

func (cache *Cache) Set(key interface{}, value interface{}) bool {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	return cache.set(key, value)
}

func (cache *Cache) set(key, value interface{}) bool {
	// keyHash 用来快速定位，conflice 用来判断冲突
	keyHash, conflictHash := cache.keyToHash(key)

	cache.inc(keyHash) //增加访问计数

	// 处理key已经存在于cache中的情况
	val, ok := cache.data[keyHash]
	if ok { // keyHash存在于data中
		item := val.Value.(*storeItem)
		if item.conflict == conflictHash { // 并且通过冲突检测
			item.value = value // 更新value值
			if item.stage == 0 {
				cache.windowbuf.get(val)
			} else {
				cache.mainbuf.get(val)
			}
			atomic.AddInt32(&cache.hits, 1)
			return true
		}
	}
	// 此时，key不在cache中，需要插入
	atomic.AddInt32(&cache.misses, 1)
	// 刚放进去的缓存都先放到 window lru 中，所以 stage = 0
	i := storeItem{
		stage:    0,
		key:      keyHash,
		conflict: conflictHash,
		value:    value,
	}

	// 如果 window 已满，要返回被淘汰的数据
	eitem, evicted := cache.windowbuf.add(i)

	if !evicted {
		return true
	}

	// 如果 window 中有被淘汰的数据，会走到这里
	// 需要从 LFU 的 stageOne 部分找到一个淘汰者
	// 二者进行 PK
	victim := cache.mainbuf.victim()

	// 走到这里是因为 LFU 未满，那么 window lru 的淘汰数据，可以进入 stageOne
	if victim == nil {
		cache.mainbuf.add(eitem)
		return true
	}

	// （ 暂时取消，因为在前面有inc(keyHash) ）这里进行 PK，必须在 bloomfilter 中出现过一次，才允许 PK
	// 在 bf 中出现，说明访问频率 >= 2
	// if !cache.filter.Allow(uint32(eitem.key)) {
	// 	return true
	// }

	// 估算 windowlru 和 LFU 中淘汰数据，历史访问频次
	// 访问频率高的，被认为更有资格留下来
	vcount := cache.counters.Estimate(victim.key)
	ocount := cache.counters.Estimate(eitem.key)

	if ocount < vcount {
		return true
	}

	// 留下来的人进入 stageOne
	cache.mainbuf.add(eitem)
	return true
}

func (cache *Cache) Get(key interface{}) (interface{}, bool) {
	cache.mutex.RLock()
	defer cache.mutex.RUnlock()
	return cache.get(key)
}

func (cache *Cache) get(key interface{}) (interface{}, bool) {
	keyHash, conflictHash := cache.keyToHash(key)

	cache.inc(keyHash)

	val, ok := cache.data[keyHash] // ok表示data中是否存在keyHash
	if !ok {                       // 未找到对应项
		atomic.AddInt32(&cache.misses, 1)
		return nil, false
	}

	item := val.Value.(*storeItem)

	if item.conflict != conflictHash { // 冲突检测不通过，因为不同的key计算得到的哈希值keyHash可能相同？
		atomic.AddInt32(&cache.misses, 1)
		return nil, false
	}
	atomic.AddInt32(&cache.hits, 1)
	v := item.value

	if item.stage == 0 {
		cache.windowbuf.get(val)
	} else {
		cache.mainbuf.get(val)
	}

	return v, true

}

func (cache *Cache) Del(key interface{}) (interface{}, bool) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	return cache.del(key)
}

func (cache *Cache) del(key interface{}) (interface{}, bool) {
	keyHash, conflictHash := cache.keyToHash(key)

	val, ok := cache.data[keyHash]
	if !ok {
		return 0, false
	}

	item := val.Value.(*storeItem)

	if conflictHash != 0 && (conflictHash != item.conflict) {
		return 0, false
	}

	delete(cache.data, keyHash)
	return item.conflict, true
}

// 增加访问计数
func (cache *Cache) inc(keyHash uint64) {
	cache.sample++
	if cache.sample == cache.sampleThreshold {
		cache.counters.Halve()
		cache.filter.clear()
		cache.sample = 0
	}
	if cache.filter.Allow(uint32(keyHash)) {
		cache.counters.Increment(keyHash)
	}
}

// 将任意类型的key映射为uint64类型的哈希值，返回(keyHash, conflictHash)
func (cache *Cache) keyToHash(key interface{}) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return xxhash.Sum64String(k), MemHashString(k) // xxhash.Sum64String(k)：这是 xxhash 库中的函数，返回字符串 k 的64位哈希值
	case []byte:
		return xxhash.Sum64(k), MemHash(k) // xxhash.Sum64(k)：这是 xxhash 库中的函数，返回字节数组 k 的64位哈希值。
	case byte:
		return uint64(k), 0
	case int:
		return uint64(k), 0
	case int32:
		return uint64(k), 0
	case uint32:
		return uint64(k), 0
	case int64:
		return uint64(k), 0
	default:
		panic("Key type not supported")
	}
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// MemHashString is the hash function used by go map, it utilizes available hardware instructions
// (behaves as aeshash if aes instruction is available).
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len))) // memhash()函数对输入的字符串或字节数组进行哈希计算
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func (cache *Cache) String() string {
	var s string
	s += cache.windowbuf.String() + " | " + cache.mainbuf.String()
	return s
}

func (cache *Cache) HitRate() float64 {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	total := cache.hits + cache.misses
	if total == 0 {
		return 0.0
	}
	return float64(cache.hits) / float64(total) * 100
}
