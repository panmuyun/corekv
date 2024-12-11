package utils

import (
	"sync/atomic"
	"unsafe"
)

const (
	offsetSize  = int(unsafe.Sizeof(uint32(0))) // 4 bytes
	memUnitSize = int(unsafe.Sizeof(uint64(0))) // 8 bytes
	MaxNodeSize = int(unsafe.Sizeof(node{}))    // size of "node{}" (bytes)
)

// MemPool(Memory Pool)
type Arena struct {
	n          uint32 // the size of the used portion in the MemPool, measured in bytes.
	shouldGrow bool   // 标记能否扩容
	buf        []byte // 存放实际的值。len(buf)是arena申请的内存空间大小
}

func newArena(arenasize int64) *Arena {
	// offset=0 的部分不使用，作为空指针
	new_arena := &Arena{
		n:   1,
		buf: make([]byte, arenasize),
	}
	return new_arena
}

// 从aerna中分配size大小的空间（可能存在arena扩容操作），并返回写入的起始偏移量
func (arena *Arena) allocate(size uint32) uint32 {
	// 分配已有的arena空间
	offset := atomic.AddUint32(&arena.n, size)
	if !arena.shouldGrow {
		AssertTrue(int(offset) <= len(arena.buf))
		return offset - size
	}
	// 扩大arena所占的空间
	if len(arena.buf)-int(offset) < MaxNodeSize {
		// arena剩余量不足以放一个node时，对arena翻倍扩容
		expand := uint32(len(arena.buf))
		if expand > 1<<30 {
			expand = 1 << 30
		}
		if expand < size {
			expand = size
		}
		new_buf := make([]byte, len(arena.buf)+int(expand)) // 一般情况下，len(new_buf) == 2*len(buf)
		AssertTrue(len(arena.buf) == copy(new_buf, arena.buf))
		arena.buf = new_buf
	}
	return offset - size
}

// 返回arena已使用空间的大小
func (arena *Arena) size() int64 {
	return int64(atomic.LoadUint32(&arena.n))
}

func (arena *Arena) putNode(nodeheight int) uint32 {
	// 注意：node中存的不是实际的key和value值，而是key、value的offset和size
	// node中的tower不会全部用到
	unused_size := (maxHeight - nodeheight) * offsetSize
	// 给node分配内存
	nodesize := uint32(MaxNodeSize - unused_size + memUnitSize - 1)
	start_offset := arena.allocate(nodesize)
	// 对node的起始偏移量进行内存对齐
	aligned_offset := (start_offset + uint32(memUnitSize-1)) & ^(uint32(memUnitSize - 1))
	return aligned_offset
}

func (arena *Arena) putKey(key []byte) uint32 {
	keysize := uint32(len(key))
	start_offset := arena.allocate(keysize)
	AssertTrue(len(key) == copy(arena.buf[start_offset:start_offset+keysize], key))
	return start_offset
}

func (arena *Arena) putVal(vs ValueStruct) uint32 {
	vssize := uint32(vs.EncodedSize())
	start_offset := arena.allocate(vssize)
	vs.EncodeValue(arena.buf[start_offset : start_offset+vssize])
	return start_offset
}

func (arena *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&arena.buf[offset]))
}

func (arena *Arena) getKey(offset uint32, size uint16) []byte {
	return arena.buf[offset : offset+uint32(size)]
}

func (arena *Arena) getVal(offset uint32, size uint32) (ret ValueStruct) {
	ret.DecodeValue(arena.buf[offset : offset+size])
	return
}

func (arena *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0
	}
	// uintptr: 将指针转换为无符号整型，便于计算偏移量offset
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&arena.buf[0])))
}
