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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/pb"
	"github.com/hardcore-os/corekv/utils"
	"github.com/pkg/errors"
)

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64 //文件的唯一标识符，方便在系统中查找和操作特定的文件
	ref int32  // For file garbage collection. Atomic.
}

// 创建SSTable对象（包括创建mmap文件，并关联一块内存区域）
// builder对象存在时，执行builder的flush方法序列化数据到SSTable。否则打开一个已经存在的SSTable文件，并对其初始化
func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	sstSize := int(lm.opt.SSTableMaxSz)
	if builder != nil {
		sstSize = int(builder.done().size)
	}
	var (
		t   *table
		err error
	)
	fid := utils.FID(tableName)
	// 对builder存在的情况 把buf flush到磁盘
	if builder != nil {
		if t, err = builder.flush(lm, tableName); err != nil {
			utils.Err(err)
			return nil
		}
	} else {
		t = &table{lm: lm, fid: fid}
		// 如果没有builder 则创打开一个已经存在的sst文件
		t.ss = file.OpenSStable(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(sstSize)}) //用于限制SStable文件的大小
	}
	// 先要引用一下，否则后面使用迭代器会导致引用状态错误
	t.IncrRef()
	//  初始化sst文件，把index加载进来
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
		return nil
	}

	// 获取sst的最大key 需要使用迭代器
	itr := t.NewIterator(&utils.Options{}) // 默认是降序
	defer itr.Close()
	// 定位到初始位置就是最大的key
	itr.Rewind()
	utils.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey")) //检查迭代器是否有效，如果无效则记录错误信息并抛出错误
	maxKey := itr.Item().Entry().Key
	t.ss.SetMaxKey(maxKey)

	return t
}

// Serach 从table中查找key
func (t *table) Serach(key []byte, maxVs *uint64) (entry *utils.Entry, err error) {
	t.IncrRef()
	defer t.DecrRef()
	// 获取索引
	idx := t.ss.Indexs()
	// 检查key是否存在
	bloomFilter := utils.Filter(idx.BloomFilter)
	if t.ss.HasBloomFilter() && !bloomFilter.MayContainKey(key) {
		return nil, utils.ErrKeyNotFound
	}
	iter := t.NewIterator(&utils.Options{})
	defer iter.Close()

	iter.Seek(key)
	if !iter.Valid() {
		return nil, utils.ErrKeyNotFound
	}

	if utils.SameKey(key, iter.Item().Entry().Key) {
		if version := utils.ParseTs(iter.Item().Entry().Key); *maxVs < version { //解析出当前key的时间戳，比较是否比maxVs大，大的话需要更新maxVs
			*maxVs = version
			return iter.Item().Entry(), nil //返回中找到的entry
		}
	}
	return nil, utils.ErrKeyNotFound
}

func (t *table) indexKey() uint64 {
	return t.fid
}
func (t *table) getEntry(key, block []byte, idx int) (entry *utils.Entry, err error) {
	if len(block) == 0 {
		return nil, utils.ErrKeyNotFound
	}
	dataStr := string(block)
	blocks := strings.Split(dataStr, ",")
	if idx >= 0 && idx < len(blocks) {
		return &utils.Entry{
			Key:   key,
			Value: []byte(blocks[idx]),
		}, nil
	}
	return nil, utils.ErrKeyNotFound
}

// 去加载sst对应的block
func (t *table) block(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx=%d", idx))
	if idx >= len(t.ss.Indexs().Offsets) {
		return nil, errors.New("block out of index")
	}
	var b *block
	key := t.blockCacheKey(idx)
	blk, ok := t.lm.cache.blocks.Get(key)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}

	var ko pb.BlockOffset
	utils.CondPanic(!t.offsets(&ko, idx), fmt.Errorf("block t.offset id=%d", idx))
	b = &block{
		offset: int(ko.GetOffset()),
	}

	var err error
	if b.data, err = t.read(b.offset, int(ko.GetLen())); err != nil {
		return nil, errors.Wrapf(err,
			"failed to read from sstable: %d at offset: %d, len: %d",
			t.ss.FID(), b.offset, ko.GetLen())
	}

	readPos := len(b.data) - 4 // First read checksum length.
	b.chkLen = int(utils.BytesToU32(b.data[readPos : readPos+4]))

	if b.chkLen > len(b.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}

	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]

	b.data = b.data[:readPos]

	if err = b.verifyCheckSum(); err != nil {
		return nil, err
	}

	readPos -= 4
	numEntries := int(utils.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + numEntries*4

	b.entryOffsets = utils.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])

	b.entriesIndexStart = entriesIndexStart

	t.lm.cache.blocks.Set(key, b)

	return b, nil
}

func (t *table) read(off, sz int) ([]byte, error) {
	return t.ss.Bytes(off, sz)
}

// blockCacheKey is used to store blocks in the block cache.
func (t *table) blockCacheKey(idx int) []byte {
	utils.CondPanic(t.fid >= math.MaxUint32, fmt.Errorf("t.fid >= math.MaxUint32"))
	utils.CondPanic(uint32(idx) >= math.MaxUint32, fmt.Errorf("uint32(idx) >=  math.MaxUint32"))

	buf := make([]byte, 8)
	// Assume t.ID does not overflow uint32.
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}

type tableIterator struct {
	it       utils.Item     // 存储当前迭代器指向的条目
	opt      *utils.Options //存储迭代器的配置选项
	t        *table         //表示当前迭代器正在遍历的表
	blockPos int            //表示当前迭代器所在的块的位置，以便在块之间进行切换
	bi       *blockIterator //指向blockIterator结构体的指针，用于在当前块内部进行迭代
	err      error          //存储在迭代过程中可能遇到的错误
}

func (t *table) NewIterator(options *utils.Options) utils.Iterator {
	t.IncrRef()
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}
func (it *tableIterator) Next() {
	it.err = nil

	if it.blockPos >= len(it.t.ss.Indexs().GetOffsets()) { //检查是否到达文件末尾
		it.err = io.EOF
		return
	}

	if len(it.bi.data) == 0 { //如果当前块没有数据，则获取下一个块的数据并初始化 blockIterator
		block, err := it.t.block(it.blockPos)
		if err != nil {
			it.err = err
			return
		}
		it.bi.tableID = it.t.fid
		it.bi.blockID = it.blockPos
		it.bi.setBlock(block)
		it.bi.seekToFirst()
		it.err = it.bi.Error()
		return
	}
	//如果当前块有数据
	it.bi.Next()        //调用 blockIterator 的 Next 方法移动到下一个条目
	if !it.bi.Valid() { //检查blockIterator 移动到下一个条目后是否仍然有效
		it.blockPos++
		it.bi.data = nil
		it.Next()
		return
	}
	it.it = it.bi.it
}

// 判断tableIterator是否还有下一个条目可以被读取
func (it *tableIterator) Valid() bool {
	return it.err != io.EOF // 如果没有的时候 则是EOF
}

// 将迭代器重置到合适的位置，以便遍历表中的条目。
// IsAsc==true：将迭代器定位到表的第一个条目  IsAsc==false：将迭代器定位到表的最后一个条目
func (it *tableIterator) Rewind() {
	if it.opt.IsAsc {
		it.seekToFirst()
	} else {
		it.seekToLast()
	}
}
func (it *tableIterator) Item() utils.Item {
	return it.it
}
func (it *tableIterator) Close() error {
	it.bi.Close()
	return it.t.DecrRef()
}

// 将迭代器定位到表的第一个条目
func (it *tableIterator) seekToFirst() {
	numBlocks := len(it.t.ss.Indexs().Offsets)
	if numBlocks == 0 { //表中没有数据时
		it.err = io.EOF
		return
	}
	it.blockPos = 0                       //将 it.blockPos 设置为 0，表示从第一个块开始
	block, err := it.t.block(it.blockPos) //获取第一个块的位置
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToFirst()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

// 将迭代器定位到表的最后一个条目
func (it *tableIterator) seekToLast() {
	numBlocks := len(it.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = numBlocks - 1
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToLast()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

// Seek：在一个有序的数据集合中查找一个特定的键（key），并定位到该键或其插入位置
// 二分法搜索 idxTables
// 如果idx == 0 说明key只能在第一个block中 block[0].MinKey <= key。否则 block[0].MinKey > key
// 如果在 idx-1 的block中未找到key 那才可能在 idx 中。如果都没有，则当前key不在此table
func (it *tableIterator) Seek(key []byte) {
	var ko pb.BlockOffset
	//sort.Search是二分查找，此处用来查找key在索引中的位置
	idx := sort.Search(len(it.t.ss.Indexs().GetOffsets()), func(idx int) bool {
		// 查找并返回第一个i,要求i满足条件：ss.idxTables.offsets[i]对应的key大于要找的key
		utils.CondPanic(!it.t.offsets(&ko, idx), fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()"))
		if idx == len(it.t.ss.Indexs().GetOffsets()) { //如果当前索引是最后一个偏移量的索引，则返回true。这通常意味着目标键大于索引中的所有键，因此需要插入在末尾
			return true
		}
		return utils.CompareKeys(ko.GetKey(), key) > 0
	})
	if idx == 0 {
		it.seekHelper(0, key)
		return
	}
	it.seekHelper(idx-1, key)
}

// 更新tableIterator.it（迭代器当前指向的条目）、tableIterator.err
func (it *tableIterator) seekHelper(blockIdx int, key []byte) {
	it.blockPos = blockIdx
	block, err := it.t.block(blockIdx)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.err = it.bi.Error()
	it.it = it.bi.Item()
}

// 将ss.idxTables.offsets[i]（索引表的第i个BlockOffset对象）存储在ko指向的BlockOffset变量中
func (t *table) offsets(ko *pb.BlockOffset, i int) bool {
	index := t.ss.Indexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	if i == len(index.GetOffsets()) {
		return true
	}
	*ko = *index.GetOffsets()[i]
	return true
}

// Size is its file size in bytes
func (t *table) Size() int64 { return int64(t.ss.Size()) }

// GetCreatedAt
func (t *table) GetCreatedAt() *time.Time {
	return t.ss.GetCreatedAt()
}
func (t *table) Delete() error {
	return t.ss.Detele()
}

// StaleDataSize is the amount of stale data (that can be dropped by a compaction )in this SST.
func (t *table) StaleDataSize() uint32 { return t.ss.Indexs().StaleDataSize }

// DecrRef decrements the refcount and possibly deletes the table
func (t *table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		// TODO 从缓存中删除
		for i := 0; i < len(t.ss.Indexs().GetOffsets()); i++ {
			t.lm.cache.blocks.Del(t.blockCacheKey(i))
		}
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}
func decrRefs(tables []*table) error {
	for _, table := range tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}
