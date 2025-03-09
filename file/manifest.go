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

package file

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/panmuyun/corekv/pb"
	"github.com/panmuyun/corekv/utils"
	"github.com/pkg/errors"
)

// ManifestFile 维护sst文件元信息的文件
// manifest 比较特殊，不能使用mmap，需要保证实时的写入
type ManifestFile struct {
	opt                       *Options
	f                         *os.File   //表示打开的文件描述符，ManifestFile使用这个文件来存储和读取SST文件的元信息
	lock                      sync.Mutex //用于在多线程环境下对ManifestFile进行操作时提供互斥访问
	deletionsRewriteThreshold int        //表示当删除操作达到这个阈值时，需要重新写入Manifest文件
	manifest                  *Manifest  //指向一个Manifest对象，用于存储实际的元信息数据
}

// Manifest corekv 元数据状态维护
type Manifest struct {
	Levels    []levelManifest          //表示不同层次的levelManifest。每一层levelManifest存储该层的SSTable信息
	Tables    map[uint64]TableManifest //一个映射，键是SSTable的ID（类型为uint64），值是TableManifest类型的结构体。
	Creations int                      //表示创建SSTable的次数
	Deletions int                      //表示删除SSTable的次数
}

// TableManifest 包含sst的基本信息
type TableManifest struct {
	Level    uint8  //表示SSTable所在的层次
	Checksum []byte // 方便今后扩展
}
type levelManifest struct {
	Tables map[uint64]struct{} // Set of table id's。这种映射通常被称为集合（set），用于快速判断一个SSTable的ID是否存在于该层中
}

// TableMeta sst 的一些元信息
type TableMeta struct {
	ID       uint64 //表示SSTable的唯一标识符
	Checksum []byte
}

// OpenManifestFile 打开manifest文件
func OpenManifestFile(opt *Options) (*ManifestFile, error) {
	path := filepath.Join(opt.Dir, utils.ManifestFilename)
	mf := &ManifestFile{lock: sync.Mutex{}, opt: opt}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	// 如果os.OpenFile返回的错误不是因为文件不存在，那么就会直接返回这个错误; 否则尝试创建一个新的 manifest file
	if err != nil {
		if !os.IsNotExist(err) {
			return mf, err
		}
		m := createManifest()
		fp, netCreations, err := helpRewrite(opt.Dir, m)
		utils.CondPanic(netCreations == 0, errors.Wrap(err, utils.ErrReWriteFailure.Error()))
		if err != nil {
			return mf, err
		}
		mf.f = fp
		f = fp
		mf.manifest = m
		return mf, nil
	}

	// 如果打开 则对manifest文件重放
	manifest, truncOffset, err := ReplayManifestFile(f)
	if err != nil {
		_ = f.Close()
		return mf, err
	}
	// Truncate file so we don't have a half-written entry at the end.
	if err := f.Truncate(truncOffset); err != nil {
		_ = f.Close()
		return mf, err
	}
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return mf, err
	}
	mf.f = f
	mf.manifest = manifest
	return mf, nil
}

// ReplayManifestFile 对已经存在的manifest文件重新应用所有状态变更
func ReplayManifestFile(fp *os.File) (ret *Manifest, truncOffset int64, err error) {
	r := &bufReader{reader: bufio.NewReader(fp)}
	//读取文件开头的8字节作为魔法字节（magicBuf），用于标识文件格式。
	//如果读取失败或者魔法字节不匹配，函数会返回一个空的Manifest对象、偏移量0和一个错误。
	var magicBuf [8]byte
	if _, err := io.ReadFull(r, magicBuf[:]); err != nil {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	if !bytes.Equal(magicBuf[0:4], utils.MagicText[:]) {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	//检查文件的版本号
	version := binary.BigEndian.Uint32(magicBuf[4:8])
	if version != uint32(utils.MagicVersion) {
		return &Manifest{}, 0,
			fmt.Errorf("manifest has unsupported version: %d (we support %d)", version, utils.MagicVersion)
	}
	//创建一个空的Manifest对象build，用于存储解析后的数据
	build := createManifest()
	var offset int64
	for { // 逐次读取并解析文件中的ChangeSet
		offset = r.count
		var lenCrcBuf [8]byte
		_, err := io.ReadFull(r, lenCrcBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		length := binary.BigEndian.Uint32(lenCrcBuf[0:4])
		var buf = make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		if crc32.Checksum(buf, utils.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCrcBuf[4:8]) {
			return &Manifest{}, 0, utils.ErrBadChecksum
		}

		var changeSet pb.ManifestChangeSet
		if err := changeSet.Unmarshal(buf); err != nil {
			return &Manifest{}, 0, err
		}

		if err := applyChangeSet(build, &changeSet); err != nil {
			return &Manifest{}, 0, err
		}
	}

	return build, offset, err
}

// This is not a "recoverable" error -- opening the KV store fails because the MANIFEST file is
// just plain broken.
// 将ChangeSet应用到Manifest对象中，补充Tables、Creations等字段的值
func applyChangeSet(build *Manifest, changeSet *pb.ManifestChangeSet) error {
	for _, change := range changeSet.Changes {
		if err := applyManifestChange(build, change); err != nil {
			return err
		}
	}
	return nil
}

func applyManifestChange(build *Manifest, tc *pb.ManifestChange) error {
	switch tc.Op {
	case pb.ManifestChange_CREATE:
		if _, ok := build.Tables[tc.Id]; ok {
			return fmt.Errorf("MANIFEST invalid, table %d exists", tc.Id)
		}
		build.Tables[tc.Id] = TableManifest{
			Level:    uint8(tc.Level),
			Checksum: append([]byte{}, tc.Checksum...),
		}
		for len(build.Levels) <= int(tc.Level) {
			build.Levels = append(build.Levels, levelManifest{make(map[uint64]struct{})})
		}
		build.Levels[tc.Level].Tables[tc.Id] = struct{}{}
		build.Creations++
	case pb.ManifestChange_DELETE:
		tm, ok := build.Tables[tc.Id]
		if !ok {
			return fmt.Errorf("MANIFEST removes non-existing table %d", tc.Id)
		}
		delete(build.Levels[tm.Level].Tables, tc.Id)
		delete(build.Tables, tc.Id)
		build.Deletions++
	default:
		return fmt.Errorf("MANIFEST file has invalid manifestChange op")
	}
	return nil
}

// 新建并返回一个Manifest对象
func createManifest() *Manifest {
	levels := make([]levelManifest, 0)
	return &Manifest{
		Levels: levels,
		Tables: make(map[uint64]TableManifest),
	}
}

type bufReader struct {
	reader *bufio.Reader
	count  int64
}

func (r *bufReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.count += int64(n)
	return
}

// asChanges returns a sequence of changes that could be used to recreate the Manifest in its
// present state.
func (m *Manifest) asChanges() []*pb.ManifestChange {
	changes := make([]*pb.ManifestChange, 0, len(m.Tables))
	for id, tm := range m.Tables {
		changes = append(changes, newCreateChange(id, int(tm.Level), tm.Checksum))
	}
	return changes
}
func newCreateChange(id uint64, level int, checksum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       id,
		Op:       pb.ManifestChange_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}

// Must be called while appendLock is held.
func (mf *ManifestFile) rewrite() error {
	// In Windows the files should be closed before doing a Rename.
	if err := mf.f.Close(); err != nil {
		return err
	}
	fp, nextCreations, err := helpRewrite(mf.opt.Dir, mf.manifest)
	if err != nil {
		return err
	}
	mf.manifest.Creations = nextCreations
	mf.manifest.Deletions = 0
	mf.f = fp
	return nil
}

// 将一个Manifest结构体的内容重写到指定目录的文件中
func helpRewrite(dir string, m *Manifest) (*os.File, int, error) {
	rewritePath := filepath.Join(dir, utils.ManifestRewriteFilename)
	// We explicitly sync.
	fp, err := os.OpenFile(rewritePath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, 8)
	copy(buf[0:4], utils.MagicText[:])                               //将utils.MagicText的前4个字节复制到buf的前4个字节位置。utils.MagicText通常是一个用于识别文件类型的魔数。
	binary.BigEndian.PutUint32(buf[4:8], uint32(utils.MagicVersion)) //将utils.MagicVersion以大端模式写入buf的后4个字节位置

	netCreations := len(m.Tables)
	changes := m.asChanges()
	set := pb.ManifestChangeSet{Changes: changes} //ManifestChangeSet用于表示Manifest文件的更改集合，记录每次更改操作

	changeBuf, err := set.Marshal() //将ManifestChangeSet实例序列化为字节切片changeBuf
	if err != nil {
		fp.Close()
		return nil, 0, err
	}
	var lenCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(changeBuf, utils.CastagnoliCrcTable))
	buf = append(buf, lenCrcBuf[:]...)
	buf = append(buf, changeBuf...)
	if _, err := fp.Write(buf); err != nil { //将buf中的内容写入到之前打开的文件中
		fp.Close()
		return nil, 0, err
	}
	if err := fp.Sync(); err != nil { //确保所有数据都被写入磁盘
		fp.Close()
		return nil, 0, err
	}

	// In Windows the files should be closed before doing a Rename.
	if err = fp.Close(); err != nil {
		return nil, 0, err
	}
	manifestPath := filepath.Join(dir, utils.ManifestFilename)
	if err := os.Rename(rewritePath, manifestPath); err != nil { //将临时文件重命名为正式的Manifest文件名
		return nil, 0, err
	}
	fp, err = os.OpenFile(manifestPath, utils.DefaultFileFlag, utils.DefaultFileMode) //重新打开文件
	if err != nil {
		return nil, 0, err
	}
	if _, err := fp.Seek(0, io.SeekEnd); err != nil { //通过将文件指针移动到文件末尾来获取文件大小
		fp.Close()
		return nil, 0, err
	}
	if err := utils.SyncDir(dir); err != nil { //同步目录。确保目录信息也被写入磁盘
		fp.Close()
		return nil, 0, err
	}

	return fp, netCreations, nil //netCreations代表表格数量
}

// Close 关闭文件
func (mf *ManifestFile) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}

// AddChanges 对外暴露的写比那更丰富
func (mf *ManifestFile) AddChanges(changesParam []*pb.ManifestChange) error {
	return mf.addChanges(changesParam)
}
func (mf *ManifestFile) addChanges(changesParam []*pb.ManifestChange) error {
	changes := pb.ManifestChangeSet{Changes: changesParam}
	buf, err := changes.Marshal()
	if err != nil {
		return err
	}

	// TODO 锁粒度可以优化
	mf.lock.Lock()
	defer mf.lock.Unlock()
	if err := applyChangeSet(mf.manifest, &changes); err != nil {
		return err
	}
	// Rewrite manifest if it'd shrink by 1/10 and it's big enough to care
	if mf.manifest.Deletions > utils.ManifestDeletionsRewriteThreshold &&
		mf.manifest.Deletions > utils.ManifestDeletionsRatio*(mf.manifest.Creations-mf.manifest.Deletions) {
		if err := mf.rewrite(); err != nil {
			return err
		}
	} else {
		var lenCrcBuf [8]byte
		binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(buf, utils.CastagnoliCrcTable))
		buf = append(lenCrcBuf[:], buf...)
		if _, err := mf.f.Write(buf); err != nil {
			return err
		}
	}
	err = mf.f.Sync()
	return err
}

// AddTableMeta 更新manifest文件：增加新change
func (mf *ManifestFile) AddTableMeta(levelNum int, t *TableMeta) (err error) {
	mf.addChanges([]*pb.ManifestChange{
		newCreateChange(t.ID, levelNum, t.Checksum),
	})
	return err
}

// RevertToManifest checks that all necessary table files exist and removes all table files not
// referenced by the manifest.  idMap is a set of table file id's that were read from the directory
// listing.
func (mf *ManifestFile) RevertToManifest(idMap map[uint64]struct{}) error {
	// 1. Check all files in manifest exist.
	for id := range mf.manifest.Tables {
		if _, ok := idMap[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}

	// 2. Delete files that shouldn't exist.
	for id := range idMap {
		if _, ok := mf.manifest.Tables[id]; !ok {
			utils.Err(fmt.Errorf("Table file %d  not referenced in MANIFEST", id))
			filename := utils.FileNameSSTable(mf.opt.Dir, id)
			if err := os.Remove(filename); err != nil {
				return errors.Wrapf(err, "While removing table %d", id)
			}
		}
	}
	return nil
}

// GetManifest manifest
func (mf *ManifestFile) GetManifest() *Manifest {
	return mf.manifest
}
