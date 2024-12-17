package utils

import "math"

// 定义了新类型Filter：是基于[]byte的（相当于[]byte的别名），并且可以为这个新类型添加方法
type Filter []byte

// 判断filter中是否可能包含了key
func (filter Filter) MayContainKey(key []byte) bool {
	return filter.MayContain(Hash(key))
}

// 判断filter中是否可能包含了hashval (uint32形态的key)
func (filter Filter) MayContain(hashval uint32) bool {
	if len(filter) < 2 {
		return false
	}
	k := filter[len(filter)-1] // 最佳哈希函数数量被存在filter的最后
	if k > 30 {
		// This is reserved for potentially new encodings for short Bloom filters.
		// Consider it a match.
		return true
	}
	nBits := uint32(8 * (len(filter) - 1)) // filter对应的bit数组的总位数
	delta := hashval>>17 | hashval<<15
	for j := uint8(0); j < k; j++ { // hashval改变k次，相当于有k个哈希函数
		bitPos := hashval % nBits
		if filter[bitPos/8]&(1<<(bitPos%8)) == 0 { // 只要有一个哈希函数值的对应bit为0, 就说明filter不包含要找的元素
			return false
		}
		hashval += delta // hashval改变一次相当于一个新的哈希函数运算结果
	}
	return true
}

// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.

// 新建一个filter, 并把keys对应的元素们记录到filter中去。（appendFilter函数的外层嵌套）
func NewFilter(keys []uint32, bitsPerKey int) Filter {
	return Filter(appendFilter(keys, bitsPerKey))
}

// 根据公式计算“ bit数组大小(m)/插入元素个数(n) ”。numEntries是插入元素个数，fp是False Positive概率
func BloomBitsPerKey(numEntries int, fp float64) int {
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(float64(0.69314718056), 2)
	locs := math.Ceil(size / float64(numEntries))
	return int(locs)
}

// 新建一个filter, 并把keys对应的元素们记录到filter中去
func appendFilter(keys []uint32, bitsPerKey int) []byte {
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	// 根据公式计算最佳的哈希函数数量。0.69 是 ln(2) 的近似值
	k := uint32(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 { // 限定哈希函数数量不超过30个
		k = 30
	}

	nBits := len(keys) * int(bitsPerKey)
	// 限定filter对应的bit数组的长度必须大于64, 从而缓解 len(keys)较小时假阳性率很高 的问题
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8 // 向上取整
	nBits = nBytes * 8
	filter := make([]byte, nBytes+1)

	for _, h := range keys {
		delta := h>>17 | h<<15
		for j := uint32(0); j < k; j++ {
			bitPos := h % uint32(nBits)
			filter[bitPos/8] |= 1 << (bitPos % 8)
			h += delta
		}
	}

	// 在filter的最后记录哈希函数的个数
	filter[nBytes] = uint8(k)

	return filter
}

// 用于计算哈希值（实现了一种类似于 Murmurhash 的哈希算法）
func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34 // 初始种子值，用于初始化哈希值，提高输出的随机性。不同的种子会产生不同的哈希序列
		m    = 0xc6a4a793 // 乘法常量
	)
	h := uint32(seed) ^ uint32(len(b))*m // 引入输入数据的长度信息，减少冲突的可能性
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24 // 位运算操作用于增加位间的依赖性
		h *= m                                                                    // 乘法操作用于扩散输入位的影响
		h ^= h >> 16                                                              // 异或操作用于混合
	}
	switch len(b) { // 处理尾部数据
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
