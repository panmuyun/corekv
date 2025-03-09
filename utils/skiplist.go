package utils

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	_ "unsafe"
)

const (
	maxHeight      = 20 // 跳表的最大高度
	heightIncrease = math.MaxUint32 / 3
)

// 跳表结点的结构体
type node struct {
	height uint16

	keySize   uint16 // key 不会更改，所以对 keySize 、keyOffset 访问不需要用原子操作
	keyOffset uint32

	value uint64 // 高 32 位存 valueSize, 低 32 位存 valueOffset

	tower [maxHeight]uint32
}

// 将 valSize 和 valOffset 合并为 value
func encodeValue(valSize uint32, valOffset uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

// 将 value 拆分为 valSize 和 valOffset
func decodeValue(value uint64) (valSize uint32, valOffset uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

// 新建一个跳表结点
func newNode(arena *Arena, key []byte, vs ValueStruct, height int) *node {
	// 分配空间
	nodeoffset := arena.putNode(height)
	keyoffset := arena.putKey(key)
	valoffset := arena.putVal(vs)
	// 记录 node 各字段的值
	nd := arena.getNode(nodeoffset)
	nd.height = uint16(height)
	nd.keySize = uint16(len(key))
	nd.keyOffset = keyoffset
	nd.value = encodeValue(vs.EncodedSize(), valoffset)
	return nd
}

// 获取 nd 结点存储的 valSize 和 valOffset
func (nd *node) getValueOffset() (uint32, uint32) {
	value := atomic.LoadUint64(&nd.value)
	return decodeValue(value)
}

// 获取 nd 结点对应的 ValueStruct 值
func (nd *node) getVs(arena *Arena) ValueStruct {
	valSize, valOffset := nd.getValueOffset()
	return arena.getVal(valOffset, valSize)
}

// 获取 nd 结点的下一個结点的 offset
func (nd *node) getNextOffset(level int) uint32 {
	return atomic.LoadUint32(&nd.tower[level])
}

// 将 nd 结点的 value 字段更改为新值 newvalue
func (nd *node) setValue(arena *Arena, newvalue uint64) { // Q:为什么参数中的arena没使用到？更新的value存到arena.buf的哪里了
	atomic.StoreUint64(&nd.value, newvalue)
}

// 比较并交换 nd.tower[level] 是否等于 oldval，若等于则替换为 newval. 否则不进行任何操作
func (nd *node) casNextOffset(level int, oldval, newval uint32) bool {
	return atomic.CompareAndSwapUint32(&nd.tower[level], oldval, newval)
}

// 获取 nd 结点对应的 key 值
func (nd *node) Key(arena *Arena) []byte {
	return arena.getKey(nd.keyOffset, nd.keySize)
}

// 跳表结构体
type Skiplist struct {
	HeadOffset uint32 // 头结点不存值;headnode.height需设置为maxHeight
	height     int32
	ref        int32 // 引用计数
	arena      *Arena
	OnClose    func()
}

// 新建一个跳表，默认height为1,ref为1；arenasize为 1<<20（1MB）
func NewSkiplist(arenasize int64) *Skiplist {
	arena := newArena(arenasize)
	headnode := newNode(arena, nil, ValueStruct{}, maxHeight)
	return &Skiplist{
		HeadOffset: arena.getNodeOffset(headnode),
		height:     1,
		ref:        1,
		arena:      arena,
	}
}

// 获取跳表的头结点
func (sl *Skiplist) getHead() *node {
	return sl.arena.getNode(sl.HeadOffset)
}

// 获取跳表中 nd 结点在 level 层的下一结点
func (sl *Skiplist) getNext(nd *node, level int) *node {
	return sl.arena.getNode(nd.getNextOffset(level))
}

// 获取跳表的高度
func (sl *Skiplist) getHeight() int32 {
	return atomic.LoadInt32(&sl.height)
}

// 找到跳表中最接近key值的结点，并返回该结点的Key值是否等于key
func (sl *Skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	cntnode := sl.getHead()
	level := int(sl.getHeight() - 1)
	for {
		nextnode := sl.getNext(cntnode, level)
		if nextnode == nil {
			// 若没到达 level 0 层，则继续向下找
			if level > 0 {
				level--
				continue
			}
			// 到达 level 0 层时，求 >key 的结点
			if !less {
				return nil, false
			}
			// 到达 level 0 层时，求 <key 的结点: 需特殊处理cntnode为头结点的情况
			if cntnode == sl.getHead() {
				return nil, false
			}
			return cntnode, false
		}

		cmp := CompareKeys(key, nextnode.Key(sl.arena))

		if cmp > 0 {
			cntnode = nextnode
			continue
		}
		if cmp == 0 {
			// 若允许等于key值，则nextnode就是要找的结点
			if allowEqual {
				return nextnode, true
			}
			// 若求 >key 的结点，则 nextnode 在 level 0 层的下一个结点就是要找的结点
			if !less {
				return sl.getNext(nextnode, 0), false
			}
			// 若求 <key 的结点，则需要继续向下层查找，直到level为0才算真正找到
			if level > 0 {
				level--
				continue
			}
			// 到达 level 0 层时，需特殊处理cntnode为头结点的情况
			if cntnode == sl.getHead() {
				return nil, false
			}
			return cntnode, false
		}
		// cmp < 0
		// 若没到达 level 0 层，则继续向下找
		if level > 0 {
			level--
			continue
		}
		// 到达 level 0 层时，求 >key 的结点
		if !less {
			return nextnode, false
		}
		// 到达 level 0 层时，求 <key 的结点: 需特殊处理cntnode为头结点的情况
		if cntnode == sl.getHead() {
			return nil, false
		}
		return cntnode, false
	}
}

// 从start指向的结点开始查找，返回满足 before.key <= key <= next.key 条件的偏移量before、next
func (sl *Skiplist) findSpliceForLevel(key []byte, start uint32, level int) (uint32, uint32) {
	before := start
	for {
		beforenode := sl.arena.getNode(before)
		next := beforenode.getNextOffset(level)
		nextnode := sl.arena.getNode(next)
		if nextnode == nil {
			return before, next
		}

		cmp := CompareKeys(key, nextnode.Key(sl.arena))

		if cmp > 0 {
			before = next
			continue
		}
		if cmp == 0 {
			return next, next
		}
		return before, next
	}
}

// 获取跳表的最后一个结点
func (sl *Skiplist) findLast() *node {
	level := sl.getHeight() - 1
	cntnode := sl.getHead()
	for {
		nextnode := sl.getNext(cntnode, int(level))
		if nextnode != nil {
			cntnode = nextnode
			continue
		}
		if level > 0 {
			level--
			continue
		}
		if cntnode == sl.getHead() {
			return nil
		}
		return cntnode
	}
}

func (sl *Skiplist) randomHeight() int {
	height := 1
	for height < maxHeight && FastRand() <= heightIncrease {
		height++
	}
	return height
}

func (sl *Skiplist) Empty() bool {
	return sl.findLast() == nil
}

func (sl *Skiplist) Add(entry *Entry) {
	key, vs := entry.Key, ValueStruct{
		Meta:      entry.Meta,
		Value:     entry.Value,
		ExpiresAt: entry.ExpiresAt,
		Version:   entry.Version,
	}

	list_height := sl.getHeight()
	var prev, next [maxHeight + 1]uint32
	prev[list_height] = sl.HeadOffset

	for i := int(list_height) - 1; i >= 0; i-- {
		prev[i], next[i] = sl.findSpliceForLevel(key, prev[i+1], i)
		// 跳表中已存在key时，将新的value存入arena中，并更新对应结点中value的偏移量
		if prev[i] == next[i] {
			valoffset := sl.arena.putVal(vs)
			value := encodeValue(vs.EncodedSize(), valoffset)
			prevnode := sl.arena.getNode(prev[i])
			prevnode.setValue(sl.arena, value)
			return
		}
	}
	// 跳表中没有已存在的key，插入新结点

	nodeheight := sl.randomHeight()
	nd := newNode(sl.arena, key, vs, nodeheight)
	// CAS更新跳表的height
	list_height = sl.getHeight()
	for nodeheight > int(list_height) {
		if atomic.CompareAndSwapInt32(&sl.height, list_height, int32(nodeheight)) { // 此处的list_height不能直接用getheight()获取，否则起不到检查的作用了
			// 更新成功后跳出循环
			break
		}
		list_height = sl.getHeight()
	}
	// 从低level向高level插入新结点
	for i := 0; i < nodeheight; i++ {
		for {
			if sl.arena.getNode(prev[i]) == nil {
				AssertTrue(i > 1)
				prev[i], next[i] = sl.findSpliceForLevel(key, sl.HeadOffset, i)
				AssertTrue(prev[i] != next[i])
			}
			nd.tower[i] = next[i]
			prevnode := sl.arena.getNode(prev[i])
			if prevnode.casNextOffset(i, next[i], sl.arena.getNodeOffset(nd)) {
				break
			}
			// CAS失败了，重新获取prev[i]和next[i]
			prev[i], next[i] = sl.findSpliceForLevel(key, prev[i], i)
			// key已存在时，仅更新val值
			if prev[i] == next[i] {
				AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				valoffset := sl.arena.putVal(vs)
				value := encodeValue(vs.EncodedSize(), valoffset)
				prevnode = sl.arena.getNode(prev[i])
				prevnode.setValue(sl.arena, value)
				return
			}
		}
	}
}

func (sl *Skiplist) Search(key []byte) ValueStruct {
	// 找到 >= key的结点
	nd, _ := sl.findNear(key, false, true)
	if nd == nil {
		return ValueStruct{}
	}
	if !SameKey(key, sl.arena.getKey(nd.keyOffset, nd.keySize)) {
		return ValueStruct{}
	}
	valsize, valoffset := nd.getValueOffset()
	vs := sl.arena.getVal(valoffset, valsize)
	return vs
}

func (sl *Skiplist) IncrRef() {
	atomic.AddInt32(&sl.ref, 1)
}

// 减少引用次数，当引用次数减为0时释放空间
func (sl *Skiplist) DecrRef() {
	newRef := atomic.AddInt32(&sl.ref, -1)
	if newRef > 0 {
		return
	}
	if sl.OnClose != nil {
		sl.OnClose()
	}
	sl.arena = nil
}

// 返回跳表大小，即内部arena使用的内存量
func (sl *Skiplist) MemSize() int64 {
	return sl.arena.size()
}

// 绘制跳表
func (sl *Skiplist) Draw(align bool) {
	reverseTree := make([][]string, sl.getHeight())
	headnode := sl.getHead()
	// 读取跳表信息存到reverseTree中
	for level := int(sl.getHeight()) - 1; level >= 0; level-- {
		nextnode := headnode
		for {
			var nodeStr string
			nextnode = sl.getNext(nextnode, level)
			if nextnode != nil {
				key := nextnode.Key(sl.arena)
				vs := nextnode.getVs(sl.arena)
				nodeStr = fmt.Sprintf("%s(%s)", key, vs.Value)
			} else {
				break
			}
			reverseTree[level] = append(reverseTree[level], nodeStr)
		}
	}
	// 使输出按结点对齐
	if align && sl.getHeight() > 1 {
		baseFloor := reverseTree[0]
		for level := 1; level < int(sl.getHeight()); level++ {
			pos := 0
			for _, ele := range baseFloor {
				if pos == len(reverseTree[level]) {
					break
				}
				if ele != reverseTree[level][pos] {
					newStr := fmt.Sprintf(strings.Repeat("-", len(ele)))
					reverseTree[level] = append(reverseTree[level][:pos+1], reverseTree[level][pos:]...)
					reverseTree[level][pos] = newStr
				}
				pos++
			}
		}
	}
	// 画图
	for level := int(sl.getHeight()) - 1; level >= 0; level-- {
		fmt.Printf("%d:", level)
		for pos, ele := range reverseTree[level] {
			if pos == len(reverseTree[level])-1 {
				fmt.Printf("%s  ", ele)
			} else {
				fmt.Printf("%s->", ele)
			}
		}
		fmt.Println()
	}
}

func (sl *Skiplist) NewSkipListIterator() Iterator {
	sl.IncrRef()
	return &SkipListIterator{
		skiplist: sl,
	}
}

// 跳表迭代器
type SkipListIterator struct {
	skiplist *Skiplist
	node     *node
}

func (iter *SkipListIterator) Rewind() {
	iter.SeekToFirst()
}

func (iter *SkipListIterator) Item() Item {
	return &Entry{
		Key:       iter.Key(),
		Value:     iter.Value().Value,
		ExpiresAt: iter.Value().ExpiresAt,
		Meta:      iter.Value().Meta,
		Version:   iter.Value().Version,
	}
}

func (iter *SkipListIterator) Close() error {
	iter.skiplist.DecrRef()
	return nil
}

func (iter *SkipListIterator) Valid() bool {
	return iter.node != nil
}

func (iter *SkipListIterator) Key() []byte {
	return iter.skiplist.arena.getKey(iter.node.keyOffset, iter.node.keySize)
}

func (iter *SkipListIterator) Value() ValueStruct {
	valsize, valoffset := iter.node.getValueOffset()
	return iter.skiplist.arena.getVal(valoffset, valsize)
}

func (iter *SkipListIterator) ValueUint64() uint64 {
	return atomic.LoadUint64(&iter.node.value)
}

func (iter *SkipListIterator) Next() {
	AssertTrue(iter.Valid())
	iter.node = iter.skiplist.getNext(iter.node, 0)
}

func (iter *SkipListIterator) Prev() {
	AssertTrue(iter.Valid())
	iter.node, _ = iter.skiplist.findNear(iter.Key(), true, false)
}

func (iter *SkipListIterator) Seek(target []byte) {
	iter.node, _ = iter.skiplist.findNear(target, false, true)
}

func (iter *SkipListIterator) SeekForPrev(target []byte) {
	iter.node, _ = iter.skiplist.findNear(target, true, true)
}

func (iter *SkipListIterator) SeekToFirst() {
	iter.node = iter.skiplist.getNext(iter.skiplist.getHead(), 0)
}

func (iter *SkipListIterator) SeekToLast() {
	iter.node = iter.skiplist.findLast()
}

type UniIterator struct {
	iter     *Iterator
	reversed bool
}

//go:linkname FastRand runtime.fastrand
func FastRand() uint32
