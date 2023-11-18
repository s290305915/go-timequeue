package timequeue

import (
	"container/list"
	"strconv"
	"time"

	"gosuc/go-timequeue/logger"
)

type Element interface {
	GetKey() string
	GetValue() interface{}
	GetTime() time.Time
}

type ElementCommon struct {
	Key      string
	Value    any
	OverTime time.Time
}

func (r *ElementCommon) GetKey() string {
	return r.Key
}

func (r *ElementCommon) GetValue() interface{} {
	return r.Value
}

func (r *ElementCommon) GetTime() time.Time {
	return r.OverTime
}

func (tq *TimeQueue) Exists(e Element) bool {
	_, exists := tq.dataMap.Load(e.GetKey())
	return exists
}

func (tq *TimeQueue) Push(bucket int, e Element) bool {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	dList, _ := tq.Buckets.Load(bucket)
	if dList == nil {
		dList = list.New()
		tq.Buckets.Store(bucket, dList)
	}

	key := e.GetKey()
	if tq.Exists(e) {
		storeElem, isOk := tq.dataMap.Load(key)
		if isOk {
			dList.(*list.List).Remove(storeElem.(*list.Element))
		}
	}

	elem := dList.(*list.List).PushBack(e)
	tq.dataMap.Store(key, elem)
	return true
}

func (tq *TimeQueue) Cancel(bucket int, key string) bool {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	dList, _ := tq.Buckets.Load(bucket)
	if dList == nil {
		return true
	}

	// 从链表中查找当前key，并进行删除
	for e := dList.(*list.List).Front(); e != nil; e = e.Next() {
		if e.Value.(Element).GetKey() != key {
			continue
		}

		storeElem, isOk := tq.dataMap.Load(key)
		if isOk {
			dList.(*list.List).Remove(storeElem.(*list.Element))
		}
		tq.dataMap.Delete(key)

		return true
	}

	return false
}

func (tq *TimeQueue) Size(bucket int) int {
	dList, _ := tq.Buckets.Load(bucket)
	return dList.(*list.List).Len()
}

func (tq *TimeQueue) GetList(bucket int) []Element {
	dList, _ := tq.Buckets.Load(bucket)

	// 遍历链表
	var data []Element
	for e := dList.(*list.List).Front(); e != nil; e = e.Next() {
		data = append(data, e.Value.(Element))
	}
	logger.Log.Info("GetList[", strconv.Itoa(bucket), "] -", strconv.Itoa(len(data)))
	return data
}

func (tq *TimeQueue) Walk(bucket int, cb func(e Element)) {
	dList, _ := tq.Buckets.Load(bucket)
	for elem := dList.(*list.List).Front(); elem != nil; elem = elem.Next() {
		cb(elem.Value.(Element))
	}
}

func (tq *TimeQueue) PopTimeout(bucket int, now time.Time) (bool, Element) {
	tq.lock.Lock()
	defer tq.lock.Unlock()

	dList, _ := tq.Buckets.Load(bucket)
	front := dList.(*list.List).Front()
	if front == nil {
		return false, nil
	}

	e := front.Value.(Element)

	if e.GetTime().Before(now) {
		dList.(*list.List).Remove(front)
		tq.dataMap.Delete(e.GetKey())
		return true, e
	}
	return false, nil
}
