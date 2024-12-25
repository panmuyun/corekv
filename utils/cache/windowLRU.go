package cache

import (
	"container/list"
	"fmt"
)

type windowLRU struct {
	data map[uint64]*list.Element // 无序；map中key的类型是uint64，value的类型是*list.Element
	cap  int                      // 容量大小
	list *list.List               // 双向链表，每次淘汰链表末尾；链表结点的Value字段存的是*storeItem类型的数据
}

type storeItem struct {
	stage    int
	key      uint64
	conflict uint64
	value    interface{} // value 字段定义为 interface{}，这意味着 value 可以存储任何类型的数据
}

func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		data: data,
		cap:  size,
		list: list.New(),
	}
}

func (wlru *windowLRU) add(newitem storeItem) (eitem storeItem, evicted bool) {
	// 如果 window 部分容量未满，直接插入
	if wlru.list.Len() < wlru.cap {
		wlru.data[newitem.key] = wlru.list.PushFront(&newitem)
		return storeItem{}, false
	}
	//如果 widow 部分容量已满，按照 lru 规则从尾部淘汰
	evictItem := wlru.list.Back()
	item := evictItem.Value.(*storeItem) // (*storeItem) 是类型断言，表示将 evictItem.Value 转换为 *storeItem 类型。

	// 从 slice 中删除该条数据
	delete(wlru.data, item.key) // 从 lru.data 这个字典中删除键为 item.key 的元素。

	// 这里直接对 evictItem 和 *item 赋值，避免向runtime 再次申请空间
	eitem, *item = *item, newitem

	wlru.data[item.key] = evictItem
	wlru.list.MoveToFront(evictItem)
	return eitem, true
}

// 将v指向的Element移到wlru.list的最前面。如果该Element不在list中，则不对list有任何操作
func (wlru *windowLRU) get(v *list.Element) {
	wlru.list.MoveToFront(v)
}

// 从前到后打印wlru.list
func (wlru *windowLRU) String() string {
	var s string
	for e := wlru.list.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	return s
}
