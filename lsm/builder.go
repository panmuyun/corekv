// Copyright 2021 hardcore-os Project Authors
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
	"io"
	"math"
	"os"
	"sort"
	"unsafe"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/pb"
	"github.com/hardcore-os/corekv/utils"
)

// 构建一个表（table）
type tableBuilder struct {
	sstSize       int64
	curBlock      *block   //指向 block 类型的指针，表示当前正在处理的 block
	opt           *Options //表示构建表时使用的各种选项或配置
	blockList     []*block //一个 block 类型指针的切片，表示所有已经构建好的 block 的列表
	keyCount      uint32   //表示当前表中键（key）的数量
	keyHashes     []uint32 //表示每个键的哈希值。用来之后生成bloomfilter
	maxVersion    uint64   //表示表中所有键的最大版本号
	baseKey       []byte   //block的第一个key作为baseKey
	staleDataSize int      //表示过时数据的大小。过时数据可能是那些需要被清理或更新的数据。
	estimateSz    int64    //表示对表最终大小的估计值
}

// 存储构建表过程中生成的数据
type buildData struct {
	blockList []*block //复制tableBuilder.blockList
	index     []byte   //存储序列化后的indexTable
	checksum  []byte   //校验和
	size      int      //记录构建的数据大小
}

type block struct {
	offset            int      //当前block的offset，当前block在文件中的起始地址
	checksum          []byte   //存储数据块的校验和
	entriesIndexStart int      //标识数据块中条目索引部分的起始位置
	chkLen            int      //校验和的长度
	data              []byte   //存储数据块的实际数据内容
	baseKey           []byte   //存储一个基础键值
	entryOffsets      []uint32 //32位无符号整数的切片，其中每个元素代表数据块中一个条目的偏移量
	end               int      //表示数据块的结束位置或者长度
	estimateSz        int64    //代表数据块的估算大小
}

type header struct {
	overlap uint16 // Overlap with base key.表示与基础键（base key）重叠的部分长度。
	diff    uint16 // Length of the diff.表示与基础键不重叠的差异部分的长度
}

const headerSize = uint16(unsafe.Sizeof(header{}))

// Decode decodes the header.使用 copy 函数将 buf 中的前 headerSize 字节复制到 header 结构体中。
func (h *header) decode(buf []byte) {
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}

// 将 header 结构体编码为字节切片
func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

// 向一个数据表中添加一个新的条目（entry）
func (tb *tableBuilder) add(e *utils.Entry, isStale bool) {
	key := e.Key
	val := utils.ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}
	// 检查是否需要分配一个新的 block
	if tb.tryFinishBlock(e) {
		if isStale {
			// This key will be added to tableIndex and it is stale.
			tb.staleDataSize += len(key) + 4 /* len */ + 4 /* offset */
		}
		tb.finishBlock()
		// Create a new block and start writing.
		tb.curBlock = &block{
			data: make([]byte, tb.opt.BlockSize), // TODO 加密block后块的大小会增加，需要预留一些填充位置
		}
	}
	tb.keyHashes = append(tb.keyHashes, utils.Hash(utils.ParseKey(key)))

	if version := utils.ParseTs(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	var diffKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = tb.keyDiff(key)
	}
	utils.CondPanic(!(len(key)-len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(key)-len(diffKey) <= math.MaxUint16"))
	utils.CondPanic(!(len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(diffKey) <= math.MaxUint16"))

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}
	// 向entryOffsets中添加这个entry的起始地址。（上一个entry的结束就是当前entry的开始）
	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	tb.append(h.encode())
	tb.append(diffKey)

	dst := tb.allocate(int(val.EncodedSize()))
	val.EncodeValue(dst)
}
func newTableBuilerWithSSTSize(opt *Options, size int64) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: size,
	}
}
func newTableBuiler(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: opt.SSTableMaxSz,
	}
}

// 根据tableBuilder对象中keyHashes的长度是否为0来判断是否为空
func (tb *tableBuilder) empty() bool { return len(tb.keyHashes) == 0 }

func (tb *tableBuilder) finish() []byte {
	bd := tb.done()
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	utils.CondPanic(written == len(buf), nil)
	return buf
}
func (tb *tableBuilder) tryFinishBlock(e *utils.Entry) bool {
	if tb.curBlock == nil {
		return true
	}

	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}
	utils.CondPanic(!((uint32(len(tb.curBlock.entryOffsets))+1)*4+4+8+4 < math.MaxUint32), errors.New("Integer overflow"))
	entriesOffsetsSize := int64((len(tb.curBlock.entryOffsets)+1)*4 +
		4 + // size of list
		8 + // Sum64 in checksum proto
		4) // checksum length
	tb.curBlock.estimateSz = int64(tb.curBlock.end) + int64(6 /*header size for entry*/) +
		int64(len(e.Key)) + int64(e.EncodedSize()) + entriesOffsetsSize

	// Integer overflow check for table size.
	utils.CondPanic(!(uint64(tb.curBlock.end)+uint64(tb.curBlock.estimateSz) < math.MaxUint32), errors.New("Integer overflow"))

	return tb.curBlock.estimateSz > int64(tb.opt.BlockSize)
}

// AddStaleKey 记录陈旧key所占用的空间大小，用于日志压缩时的决策
func (tb *tableBuilder) AddStaleKey(e *utils.Entry) {
	// Rough estimate based on how much space it will occupy in the SST.
	tb.staleDataSize += len(e.Key) + len(e.Value) + 4 /* entry offset */ + 4 /* header size */
	tb.add(e, true)
}

// AddKey _
func (tb *tableBuilder) AddKey(e *utils.Entry) {
	tb.add(e, false)
}

// Close closes the TableBuilder.
func (tb *tableBuilder) Close() {
	// 结合内存分配器
}

// 序列化当前block；
// 将curBlock的entryOffsets、entryOffsets_len、checksum、checksum_len以字节切片或字节的格式追加到tableBuilder.curBlock.data中；
// 更新tableBuilder的estimateSz、blockList、keyCount、curBlock属性
func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	// Append the entryOffsets and its length.
	tb.append(utils.U32SliceToBytes(tb.curBlock.entryOffsets)) //将当前数据块中entryOffsets切片转换为字节切片，并将其追加到
	tb.append(utils.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])

	// Append the block checksum and its length.
	tb.append(checksum)
	tb.append(utils.U32ToBytes(uint32(len(checksum))))
	tb.estimateSz += tb.curBlock.estimateSz
	tb.blockList = append(tb.blockList, tb.curBlock)
	// TODO: 预估整理builder写入磁盘后，sst文件的大小
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	tb.curBlock = nil // 表示当前block 已经被序列化到内存
	return
}

// 把data的内容追加到tableBuilder.curBlock.data中
func (tb *tableBuilder) append(data []byte) {
	dst := tb.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}

// 从tableBuilder.curBlock.data中分配need大小的空间，更新curblock.end，并返回这段空间对应的字节切片
func (tb *tableBuilder) allocate(need int) []byte {
	bb := tb.curBlock
	if len(bb.data[bb.end:]) < need {
		// We need to reallocate.
		sz := 2 * len(bb.data)
		if bb.end+need > sz {
			sz = bb.end + need
		}
		tmp := make([]byte, sz) // todo 这里可以使用内存分配器来提升性能
		copy(tmp, bb.data)
		bb.data = tmp
	}
	bb.end += need
	return bb.data[bb.end-need : bb.end]
}

func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checkSum := utils.CalculateChecksum(data)
	return utils.U64ToBytes(checkSum)
}

// 返回newKey与baseKey有差异的部分
func (tb *tableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(tb.curBlock.baseKey); i++ {
		if newKey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

// TODO: 这里存在多次的用户空间拷贝过程，需要优化
func (tb *tableBuilder) flush(lm *levelManager, tableName string) (t *table, err error) {
	bd := tb.done()
	t = &table{lm: lm, fid: utils.FID(tableName)}

	t.ss = file.OpenSStable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(bd.size)})
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	utils.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))
	dst, err := t.ss.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}
	copy(dst, buf)
	return t, nil
}

// 将blockList中多个数据块的内容、index、index_len的字节形式、checksum、checksum_len的字节形式复制到一个目标字节切片 dst 中。返回已写入的字节数
func (bd *buildData) Copy(dst []byte) int {
	var written int
	for _, bl := range bd.blockList {
		written += copy(dst[written:], bl.data[:bl.end])
	}
	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.index))))

	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.checksum))))
	return written
}

// 完成SST文件的构建，并返回一个包含构建数据信息的buildData结构体
func (tb *tableBuilder) done() buildData {
	tb.finishBlock()
	if len(tb.blockList) == 0 {
		return buildData{}
	}
	bd := buildData{
		blockList: tb.blockList,
	}

	var f utils.Filter
	if tb.opt.BloomFalsePositive > 0 {
		bits := utils.BloomBitsPerKey(len(tb.keyHashes), tb.opt.BloomFalsePositive)
		f = utils.NewFilter(tb.keyHashes, bits)
	}
	// TODO 构建 sst的索引
	index, dataSize := tb.buildIndex(f)
	checksum := tb.calculateChecksum(index)
	bd.index = index
	bd.checksum = checksum
	bd.size = int(dataSize) + len(index) + len(checksum) + 4 + 4
	return bd
}

// 对tableBuilder.blockList对象序列化后，按序存到tableIndex.Offsets中。返回indexTable的序列化结果
func (tb *tableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	tableIndex.KeyCount = tb.keyCount
	tableIndex.MaxVersion = tb.maxVersion
	tableIndex.Offsets = tb.writeBlockOffsets(tableIndex)
	var dataSize uint32
	for i := range tb.blockList {
		dataSize += uint32(tb.blockList[i].end)
	}
	data, err := tableIndex.Marshal()
	utils.Panic(err)
	return data, dataSize
}

func (tb *tableBuilder) writeBlockOffsets(tableIndex *pb.TableIndex) []*pb.BlockOffset {
	var startOffset uint32
	var offsets []*pb.BlockOffset
	for _, bl := range tb.blockList {
		offset := tb.writeBlockOffset(bl, startOffset)
		offsets = append(offsets, offset)
		startOffset += uint32(bl.end)
	}
	return offsets
}

func (b *tableBuilder) writeBlockOffset(bl *block, startOffset uint32) *pb.BlockOffset {
	offset := &pb.BlockOffset{}
	offset.Key = bl.baseKey
	offset.Len = uint32(bl.end)
	offset.Offset = startOffset
	return offset
}

// TODO: 如何能更好的预估builder的长度呢？
func (b *tableBuilder) ReachedCapacity() bool {
	return b.estimateSz > b.sstSize
}

func (b block) verifyCheckSum() error {
	return utils.VerifyChecksum(b.data, b.checksum)
}

type blockIterator struct {
	data         []byte   //存储了数据块的具体内容
	idx          int      //表示当前迭代器在数据块中的位置或索引
	err          error    //用于存储在迭代过程中可能遇到的任何错误
	baseKey      []byte   //表示当前迭代的数据块的基准键，这个键可以用于构建其他键
	key          []byte   //存储了当前迭代到的键
	val          []byte   //存储了与当前键对应的值
	entryOffsets []uint32 //uint32 类型的切片，存储了数据块中每个条目的偏移量
	block        *block   //指向 block 类型的指针，指向当前正在迭代的数据块

	tableID uint64 //表示数据表的ID，用于标识数据块所属的表
	blockID int    //表示数据块的ID，用于标识数据块在表中的位置

	prevOverlap uint16 //用于存储前一个键与baseKey的重叠部分的长度

	it utils.Item //存储当前迭代到的键值对
}

// 根据block对象设置blockIterator对象
func (itr *blockIterator) setBlock(b *block) {
	itr.block = b
	itr.err = nil
	itr.idx = 0
	itr.baseKey = itr.baseKey[:0]
	itr.prevOverlap = 0
	itr.key = itr.key[:0]
	itr.val = itr.val[:0]
	// Drop the index from the block. We don't need it anymore.
	itr.data = b.data[:b.entriesIndexStart]
	itr.entryOffsets = b.entryOffsets
}

// seekToFirst brings us to the first element.
func (itr *blockIterator) seekToFirst() {
	itr.setIdx(0)
}
func (itr *blockIterator) seekToLast() {
	itr.setIdx(len(itr.entryOffsets) - 1)
}

// 将blockIterator中与entry相关的属性修改为key对应的entry
func (itr *blockIterator) seek(key []byte) {
	itr.err = nil
	startIndex := 0 // This tells from which index we should start binary search.

	foundEntryIdx := sort.Search(len(itr.entryOffsets), func(idx int) bool {
		// If idx is less than start index then just return false.
		if idx < startIndex {
			return false
		}
		itr.setIdx(idx)
		return utils.CompareKeys(itr.key, key) >= 0
	})
	itr.setIdx(foundEntryIdx)
}

// 更新blockIterator中与当前entry相关的属性
func (itr *blockIterator) setIdx(i int) {
	itr.idx = i
	if i >= len(itr.entryOffsets) || i < 0 {
		itr.err = io.EOF
		return
	}
	itr.err = nil
	startOffset := int(itr.entryOffsets[i])

	// Set base key. baseKey=第一个entry的diffKey
	if len(itr.baseKey) == 0 {
		var baseHeader header
		baseHeader.decode(itr.data)
		itr.baseKey = itr.data[headerSize : headerSize+baseHeader.diff]
	}

	var endOffset int
	// idx points to the last entry in the block.
	if itr.idx+1 == len(itr.entryOffsets) {
		endOffset = len(itr.data)
	} else {
		// idx point to some entry other than the last one in the block.
		// EndOffset of the current entry is the start offset of the next entry.
		endOffset = int(itr.entryOffsets[itr.idx+1])
	}
	defer func() {
		if r := recover(); r != nil {
			var debugBuf bytes.Buffer
			fmt.Fprintf(&debugBuf, "==== Recovered====\n")
			fmt.Fprintf(&debugBuf, "Table ID: %d\nBlock ID: %d\nEntry Idx: %d\nData len: %d\n"+
				"StartOffset: %d\nEndOffset: %d\nEntryOffsets len: %d\nEntryOffsets: %v\n",
				itr.tableID, itr.blockID, itr.idx, len(itr.data), startOffset, endOffset,
				len(itr.entryOffsets), itr.entryOffsets)
			panic(debugBuf.String())
		}
	}()

	entryData := itr.data[startOffset:endOffset]
	var h header
	h.decode(entryData)
	if h.overlap > itr.prevOverlap {
		itr.key = append(itr.key[:itr.prevOverlap], itr.baseKey[itr.prevOverlap:h.overlap]...)
	}

	itr.prevOverlap = h.overlap
	valueOff := headerSize + h.diff
	diffKey := entryData[headerSize:valueOff]
	itr.key = append(itr.key[:h.overlap], diffKey...) //在上一个key的基础上更改
	e := &utils.Entry{Key: itr.key}
	val := &utils.ValueStruct{}
	val.DecodeValue(entryData[valueOff:])
	itr.val = val.Value
	e.Value = val.Value
	e.ExpiresAt = val.ExpiresAt
	e.Meta = val.Meta
	itr.it = &Item{e: e}
}

func (itr *blockIterator) Error() error {
	return itr.err
}

func (itr *blockIterator) Next() {
	itr.setIdx(itr.idx + 1)
}

func (itr *blockIterator) Valid() bool {
	return itr.err != io.EOF // TODO 这里用err比较好
}
func (itr *blockIterator) Rewind() bool {
	itr.setIdx(0)
	return true
}
func (itr *blockIterator) Item() utils.Item {
	return itr.it
}
func (itr *blockIterator) Close() error {
	return nil
}
