package utils

import (
	"encoding/binary"
	"time"
)

// ValueStruct存放Entry中需要持久化的字段
type ValueStruct struct {
	Meta      byte
	ExpiresAt uint64 // 过期时间
	Value     []byte

	Version uint64 // This field is not serialized. Only for internal usage.
}

// 只持久化Meta、过期时间、具体的Value值
func (vs *ValueStruct) EncodedSize() uint32 {
	// “1”指的是Meta所占的字节数
	return uint32(1 + sizeVarint(vs.ExpiresAt) + len(vs.Value))
}

// 将ValueStruct的各字段值存入Arena的buf中，其中ExpiresAt字段的值经varint编码后再存入
// 存放顺序：Meta、ExpiresAt的varint编码值、Value
func (vs *ValueStruct) EncodeValue(b []byte) uint32 {
	b[0] = vs.Meta
	expsize := binary.PutUvarint(b[1:], vs.ExpiresAt)
	valsize := copy(b[1+expsize:], vs.Value)
	return uint32(1 + expsize + valsize)
}

// 从Arena的buf中取出ValueStruct的各字段值，并对ExpiresAt的varint编码值解码
func (vs *ValueStruct) DecodeValue(buf []byte) {
	vs.Meta = buf[0]
	var expsize int
	vs.ExpiresAt, expsize = binary.Uvarint(buf[1:])
	vs.Value = buf[1+expsize:]
}

// 返回x的varint编码值需要占用的字节数
func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

// Entry _ 最外层写入的结构体
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Meta         byte
	Version      uint64
	Offset       uint32
	Hlen         int // Length of the header.
	ValThreshold int64
}

// NewEntry_
func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

// Entry_
func (e *Entry) Entry() *Entry {
	return e
}

func (e *Entry) IsDeletedOrExpired() bool {
	if e.Value == nil {
		return true
	}

	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}

// WithTTL _
func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

// EncodedSize is the size of the ValueStruct when encoded
func (e *Entry) EncodedSize() uint32 {
	sz := len(e.Value)
	enc := sizeVarint(uint64(e.Meta))
	enc += sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}

// EstimateSize
func (e *Entry) EstimateSize(threshold int) int {
	// TODO: 是否考虑 user meta?
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 1 // Meta
	}
	return len(e.Key) + 12 + 1 // 12 for ValuePointer, 2 for meta.
}

// header 对象
// header is used in value log as a header before Entry.
type Header struct {
	KLen      uint32
	VLen      uint32
	ExpiresAt uint64
	Meta      byte
}

// +------+----------+------------+--------------+-----------+
// | Meta | UserMeta | Key Length | Value Length | ExpiresAt |
// +------+----------+------------+--------------+-----------+
func (h Header) Encode(out []byte) int {
	out[0] = h.Meta
	index := 1
	index += binary.PutUvarint(out[index:], uint64(h.KLen))
	index += binary.PutUvarint(out[index:], uint64(h.VLen))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}

// Decode decodes the given header from the provided byte slice.
// Returns the number of bytes read.
func (h *Header) Decode(buf []byte) int {
	h.Meta = buf[0]
	index := 1
	klen, count := binary.Uvarint(buf[index:])
	h.KLen = uint32(klen)
	index += count
	vlen, count := binary.Uvarint(buf[index:])
	h.VLen = uint32(vlen)
	index += count
	h.ExpiresAt, count = binary.Uvarint(buf[index:])
	return index + count
}

// DecodeFrom reads the header from the hashReader.
// Returns the number of bytes read.
func (h *Header) DecodeFrom(reader *HashReader) (int, error) {
	var err error
	h.Meta, err = reader.ReadByte()
	if err != nil {
		return 0, err
	}
	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KLen = uint32(klen)
	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.VLen = uint32(vlen)
	h.ExpiresAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.BytesRead, nil
}
