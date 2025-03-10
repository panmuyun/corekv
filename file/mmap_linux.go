//go:build linux
// +build linux

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
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/panmuyun/corekv/utils/mmap"
	"github.com/pkg/errors"
)

// MmapFile represents an mmapd file and includes both the buffer to the data and the file descriptor.
type MmapFile struct {
	Data []byte   //存储文件的内存映射数据
	Fd   *os.File //指向 os.File 类型的指针，表示与内存映射文件相关联的文件描述符
}

// OpenMmapFileUsing os：通过内存映射（mmap）的方式打开一个文件，并返回一个 MmapFile 结构体指针以及可能的错误
func OpenMmapFileUsing(fd *os.File, sz int, writable bool) (*MmapFile, error) {
	filename := fd.Name()
	fi, err := fd.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat file: %s", filename)
	}

	var rerr error
	fileSize := fi.Size()
	if sz > 0 && fileSize == 0 {
		// If file is empty, truncate it to sz.将文件截断到指定的大小sz
		if err := fd.Truncate(int64(sz)); err != nil {
			return nil, errors.Wrapf(err, "error while truncation")
		}
		fileSize = int64(sz)
	}

	// fmt.Printf("Mmaping file: %s with writable: %v filesize: %d\n", fd.Name(), writable, fileSize)
	buf, err := mmap.Mmap(fd, writable, fileSize) // Mmap up to file size.将文件映射到内存中
	if err != nil {
		return nil, errors.Wrapf(err, "while mmapping %s with size: %d", fd.Name(), fileSize)
	}
	//调用 SyncDir 确保目录的元数据被持久化。不同步文件内容
	if fileSize == 0 {
		dir, _ := filepath.Split(filename)
		go SyncDir(dir)
	}
	return &MmapFile{
		Data: buf,
		Fd:   fd,
	}, rerr
}

// OpenMmapFile opens an existing file or creates a new file. If the file is
// created, it would truncate the file to maxSz. In both cases, it would mmap
// the file to maxSz and returned it. In case the file is created, z.NewFile is
// returned.
func OpenMmapFile(filename string, flag int, maxSz int) (*MmapFile, error) {
	// fmt.Printf("opening file %s with flag: %v\n", filename, flag)
	fd, err := os.OpenFile(filename, flag, 0666) //以指定的标志 flag 打开或创建文件 filename。权限 0666 表示文件所有者、组用户和其他用户都有读写权限。
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open: %s", filename)
	}
	writable := true
	if flag == os.O_RDONLY {
		writable = false
	}
	// 如果 sst文件层被打开过，则使用其文件原来的大小
	if fileInfo, err := fd.Stat(); err == nil && fileInfo != nil && fileInfo.Size() > 0 {
		maxSz = int(fileInfo.Size()) //如果文件已经存在并且有大小，那么将 maxSz 设置为文件的实际大小
	}
	return OpenMmapFileUsing(fd, maxSz, writable)
}

type mmapReader struct {
	Data   []byte //表示内存映射文件的数据
	offset int    //表示当前读取的位置或偏移量
}

// 从内存映射文件中读取数据到提供的缓冲区buf中。返回两个值：第一个是读取的字节数n，第二个是可能发生的错误
func (mr *mmapReader) Read(buf []byte) (int, error) {
	if mr.offset > len(mr.Data) {
		return 0, io.EOF
	}
	n := copy(buf, mr.Data[mr.offset:])
	mr.offset += n
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func (m *MmapFile) NewReader(offset int) io.Reader {
	return &mmapReader{
		Data:   m.Data,
		offset: offset,
	}
}

// Bytes returns data starting from offset off of size sz. If there's not enough data, it would
// return nil slice and io.EOF.
func (m *MmapFile) Bytes(off, sz int) ([]byte, error) {
	if len(m.Data[off:]) < sz {
		return nil, io.EOF
	}
	return m.Data[off : off+sz], nil
}

// Slice returns the slice at the given offset.
func (m *MmapFile) Slice(offset int) []byte {
	sz := binary.BigEndian.Uint32(m.Data[offset:]) //从 m.Data 的 offset 位置开始读取 4 个字节，并将其解析为一个无符号 32 位整数 sz。
	start := offset + 4                            //计算实际数据的起始位置，跳过前 4 个字节
	next := start + int(sz)
	if next > len(m.Data) {
		return []byte{}
	}
	res := m.Data[start:next]
	return res
}

// AllocateSlice allocates a slice of the given size at the given offset.
// 在内存映射的文件中分配一个指定大小的字节数组，并返回该字节数组、其结束位置以及可能的错误
func (m *MmapFile) AllocateSlice(sz, offset int) ([]byte, int, error) {
	start := offset + 4

	// If the file is too small, double its size or increase it by 1GB, whichever is smaller.
	if start+sz > len(m.Data) {
		const oneGB = 1 << 30 //表示1GB的字节数
		growBy := len(m.Data) //表示需要扩展的大小
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < sz+4 {
			growBy = sz + 4
		}
		if err := m.Truncature(int64(len(m.Data) + growBy)); err != nil { //调用m.Truncature方法，尝试将文件的大小增加growBy字节
			return nil, 0, err
		}
	}

	binary.BigEndian.PutUint32(m.Data[offset:], uint32(sz)) //将分配的内存大小sz以大端字节序写入到m.Data[offset:]的位置
	return m.Data[start : start+sz], start + sz, nil
}

const oneGB = 1 << 30

// AppendBuffer 向内存中追加一个buffer，如果空间不足则重新映射，扩大空间
func (m *MmapFile) AppendBuffer(offset uint32, buf []byte) error {
	size := len(m.Data)
	needSize := len(buf)
	end := int(offset) + needSize
	if end > size {
		growBy := size
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < needSize {
			growBy = needSize
		}
		if err := m.Truncature(int64(end)); err != nil {
			return err
		}
	}
	dLen := copy(m.Data[offset:end], buf)
	if dLen != needSize {
		return errors.Errorf("dLen != needSize AppendBuffer failed")
	}
	return nil
}

// 对映射内存的修改需要手动同步到文件（调用mmap.Msync）
func (m *MmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return mmap.Msync(m.Data)
}

// 删除一个通过内存映射打开的文件。它首先取消内存映射，然后清空文件内容，接着关闭文件描述符，最后删除文件本身。
func (m *MmapFile) Delete() error {
	if m.Fd == nil {
		return nil
	}

	if err := mmap.Munmap(m.Data); err != nil { //调用 mmap.Munmap 函数来取消内存映射
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	m.Data = nil
	if err := m.Fd.Truncate(0); err != nil { //将文件截断为长度为 0，即清空文件内容
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := m.Fd.Close(); err != nil { //关闭文件描述符
		return fmt.Errorf("while close file: %s, error: %v\n", m.Fd.Name(), err)
	}
	return os.Remove(m.Fd.Name()) //删除文件
}

// Close would close the file. It would also truncate the file if maxSz >= 0.
func (m *MmapFile) Close() error {
	if m.Fd == nil {
		return nil
	}
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	return m.Fd.Close()
}

// 确保对目录 dir 的所有文件系统更改都被写入磁盘，并在完成后正确关闭该目录
func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "while opening %s", dir)
	}
	if err := df.Sync(); err != nil { //调用 df.Sync 方法来同步目录 dir 的文件系统更改
		return errors.Wrapf(err, "while syncing %s", dir)
	}
	if err := df.Close(); err != nil { //调用 df.Close 方法来关闭目录 dir。如果关闭过程中发生错误，err 将会包含错误信息
		return errors.Wrapf(err, "while closing %s", dir)
	}
	return nil
}

// Truncature 兼容接口：对一个内存映射的文件进行截断和重新映射
func (m *MmapFile) Truncature(maxSz int64) error {
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := m.Fd.Truncate(maxSz); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}

	var err error
	m.Data, err = mmap.Mremap(m.Data, int(maxSz)) // Mmap up to max size.
	return err
}

// ReName 兼容接口
func (m *MmapFile) ReName(name string) error {
	return nil
}
