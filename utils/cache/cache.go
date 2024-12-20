package cache

import (
	"container/list"
	"sync"
	"unsafe"

	xxhash "github.com/cespare/xxhash/v2"
)

type Cache struct {
	mutex     sync.RWMutex
	wlru      *windowLRU
	slru      *segmentedLRU
	door      *BloomFilter
	cmsketch  *cmSketch // 记录访问计数
	t         int32
	threshold int32
	data      map[uint64]*list.Element
}

type Options struct {
	wlruPct uint8
}

// NewCache size 指的是要缓存的数据个数
func NewCache(size int) *Cache {
	//定义 Window-LRU 部分缓存所占百分比，这里定义为1%
	const wlruPct = 1
	//计算出来 window 部分的容量
	wlruSz := (wlruPct * size) / 100

	if wlruSz < 1 {
		wlruSz = 1
	}

	// 计算 segmented-LFU 部分的缓存容量
	slruSz := int(float64(size) * ((100 - wlruPct) / 100.0))

	if slruSz < 1 {
		slruSz = 1
	}

	//LFU 分为两部分，stageOne 部分占比20%
	slruO := int(0.2 * float64(slruSz))

	if slruO < 1 {
		slruO = 1
	}

	data := make(map[uint64]*list.Element, size)

	return &Cache{
		wlru:     newWindowLRU(wlruSz, data),
		slru:     newSLRU(data, slruO, slruSz-slruO),
		door:     newFilter(size, 0.01), //布隆过滤器设置误差率为0.01
		cmsketch: newCmSketch(int64(size)),
		data:     data, //共用同一个 map 存储数据
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

	// 刚放进去的缓存都先放到 window lru 中，所以 stage = 0
	i := storeItem{
		stage:    0,
		key:      keyHash,
		conflict: conflictHash,
		value:    value,
	}

	// 如果 window 已满，要返回被淘汰的数据
	eitem, evicted := cache.wlru.add(i)

	if !evicted {
		return true
	}

	// 如果 window 中有被淘汰的数据，会走到这里
	// 需要从 LFU 的 stageOne 部分找到一个淘汰者
	// 二者进行 PK
	victim := cache.slru.victim()

	// 走到这里是因为 LFU 未满，那么 window lru 的淘汰数据，可以进入 stageOne
	if victim == nil {
		cache.slru.add(eitem)
		return true
	}

	// 这里进行 PK，必须在 bloomfilter 中出现过一次，才允许 PK
	// 在 bf 中出现，说明访问频率 >= 2
	if !cache.door.Allow(uint32(eitem.key)) {
		return true
	}

	// 估算 windowlru 和 LFU 中淘汰数据，历史访问频次
	// 访问频率高的，被认为更有资格留下来
	vcount := cache.cmsketch.Estimate(victim.key)
	ocount := cache.cmsketch.Estimate(eitem.key)

	if ocount < vcount {
		return true
	}

	// 留下来的人进入 stageOne
	cache.slru.add(eitem)
	return true
}

func (cache *Cache) Get(key interface{}) (interface{}, bool) {
	cache.mutex.RLock()
	defer cache.mutex.RUnlock()
	return cache.get(key)
}

func (cache *Cache) get(key interface{}) (interface{}, bool) {
	cache.t++
	if cache.t == cache.threshold {
		cache.cmsketch.Reset()
		cache.door.reset()
		cache.t = 0
	}

	keyHash, conflictHash := cache.keyToHash(key)

	val, ok := cache.data[keyHash]
	if !ok {
		cache.door.Allow(uint32(keyHash))
		cache.cmsketch.Increment(keyHash)
		return nil, false
	}

	item := val.Value.(*storeItem)

	if item.conflict != conflictHash {
		cache.door.Allow(uint32(keyHash))
		cache.cmsketch.Increment(keyHash)
		return nil, false
	}
	cache.door.Allow(uint32(keyHash))
	cache.cmsketch.Increment(item.key)

	v := item.value

	if item.stage == 0 {
		cache.wlru.get(val)
	} else {
		cache.slru.get(val)
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

func (cache *Cache) keyToHash(key interface{}) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		return MemHash(k), xxhash.Sum64(k)
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
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func (cache *Cache) String() string {
	var s string
	s += cache.wlru.String() + " | " + cache.slru.String()
	return s
}
