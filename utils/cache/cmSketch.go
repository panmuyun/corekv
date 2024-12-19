package cache

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	cmDepth = 4 // 哈希函数个数
)

type cmSketch struct {
	rows [cmDepth]cmRow // 每个cmRow记录一个哈希函数的访问计数，共cmDepth个哈希函数
	seed [cmDepth]uint64
	mask uint64
}

func newCmSketch(numCounters int64) *cmSketch {
	if numCounters == 0 {
		panic("cmSketch: invalid numCounters")
	}

	// numCounters 一定是二次幂，也就一定是1后面有 n 个 0
	numCounters = next2Power(numCounters)
	// mask 一定是0111...111
	sketch := &cmSketch{mask: uint64(numCounters - 1)}
	source := rand.New(rand.NewSource(time.Now().UnixNano())) // 创建一个随机数生成器

	// 初始化4行
	// 0000,0000|0000,0000|0000,0000
	// 0000,0000|0000,0000|0000,0000
	// 0000,0000|0000,0000|0000,0000
	// 0000,0000|0000,0000|0000,0000

	for i := 0; i < cmDepth; i++ {
		sketch.seed[i] = source.Uint64()
		sketch.rows[i] = newCmRow(numCounters)
	}

	return sketch
}

func (s *cmSketch) Increment(hashed uint64) {
	// 对于每一行进行相同操作
	for i := range s.rows {
		s.rows[i].increment((hashed ^ s.seed[i]) & s.mask)
	}
}

// 找到最小的计数值
func (s *cmSketch) Estimate(hashed uint64) int64 {
	min := byte(255)
	for i := range s.rows {
		val := s.rows[i].get((hashed ^ s.seed[i]) & s.mask)
		if val < min {
			min = val
		}
	}

	return int64(min)
}

func (s *cmSketch) Reset() {
	for _, r := range s.rows {
		r.reset()
	}
}

func (s *cmSketch) Clear() {
	for _, r := range s.rows {
		r.clear()
	}
}

// 快速计算大于 X，且最接近 X 的二次幂
func next2Power(x int64) int64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}

// 一个BitMap的实现，用于一个哈希函数的访问计数
type cmRow []byte // 一个byte有8位，一个计数器需要4bit,所以一个byte可以当作2个计数器（counter）

// 新建一个计数器个数为numCounters的访问计数数组
func newCmRow(numCounters int64) cmRow {
	return make(cmRow, numCounters/2)
}

// 获取n对应位置的counter的计数值
func (r cmRow) get(n uint64) byte {
	return r[n/2] >> ((n & 1) * 4) & 0x0f
}

// 累加计数，n是key的哈希值
func (r cmRow) increment(n uint64) {
	i := n / 2                      // 定位到第i个byte
	shift := (n & 1) * 4            // n & 1用于判断n是奇数还是偶数，所以n为奇数时s为4，n为偶数时s为0
	count := (r[i] >> shift) & 0x0f // count为n对应位置的计数值
	// 没有超过最大计数时，计数+1
	if count < 15 {
		r[i] += 1 << shift
	}
}

// 保鲜机制，将所有counter的计数减半
func (r cmRow) reset() {
	for i := range r {
		r[i] = (r[i] >> 1) & 0x77 // 0x77: 0111 0111,使得每个4bit的最高位置0.
	}
}

// 将所有counter的计数清零
func (r cmRow) clear() {
	for i := range r {
		r[i] = 0
	}
}

// 显示所有counter的计数值
func (r cmRow) string() string {
	str := ""
	for i := uint64(0); i < uint64(len(r)*2); i++ {
		str += fmt.Sprintf("%02d ", (r[(i/2)]>>((i&1)*4))&0x0f)
	}
	str = str[:len(str)-1]
	return str
}
