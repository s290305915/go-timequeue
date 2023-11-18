package timequeue

import (
	"container/list"
	"sync"
	"time"

	"gosuc/go-timequeue/logger"
	"gosuc/go-timequeue/utils"
)

// ==============================================================================================================================

type ExpireDataHandler interface {
	HandleExpireData(key string, data interface{}, age int) error
}

type TimeQueue struct {
	QueueName string             // 队列名称
	bucketNum int                // 桶数量
	Buckets   sync.Map           // map[int]*list.List 按桶进行扫描，提高扫描效率（值为链表）
	dataMap   sync.Map           // 存储键值
	Bloom     *utils.BloomFilter // 布隆过滤器，用于处理key值hash和数据存在问题判断
	lock      sync.Mutex         // 并发锁

	Handler ExpireDataHandler // 自定义过期数据处理器
}

var Queues map[string]*TimeQueue // 时间队列

func NewTimeQueue(queueName string, bucketNum int) *TimeQueue {
	if Queues == nil {
		Queues = make(map[string]*TimeQueue)
	}

	Queue := &TimeQueue{
		QueueName: queueName,
		bucketNum: bucketNum,
		dataMap:   sync.Map{}, // *list.Element
		Buckets:   sync.Map{}, // *list.List
		Bloom:     utils.NewBloom(bucketNum),
	}

	for i := 0; i < bucketNum; i++ {
		// 预先分配桶
		dataList := list.New()
		Queue.Buckets.Store(i, dataList) // 存放 *list.Element 推送数据的链表指针

	}
	Queues[queueName] = Queue
	return Queue
}

func Use(queueName string) *TimeQueue {
	if Queues == nil {
		Queues = make(map[string]*TimeQueue)
	}

	if _, ok := Queues[queueName]; !ok {
		panic("队列不存在")
	}

	return Queues[queueName]
}

func (tq *TimeQueue) PushQueue(key string, age int, data interface{}) error {
	BlObj := utils.GetHashBloom(key)

	bucket := BlObj.Bucket

	pushData := &ElementCommon{
		Key:      key,
		Value:    data,
		OverTime: time.Now().Add(time.Duration(age) * time.Second),
	}

	toTimeStr := pushData.OverTime.Format("2006-01-02 15:04:05")

	logger.Log.Info("["+key+"]写入，过期时间：", toTimeStr, "，数据：", data)

	tq.Push(bucket, pushData)

	return nil
}

func (tq *TimeQueue) CancelQueue(key string) bool {
	BlObj := utils.GetHashBloom(key)
	isExt := tq.Bloom.ExistsBloom(key)
	if !isExt {
		return false
	}

	bucket := BlObj.Bucket

	isOk := tq.Cancel(bucket, key)
	return isOk
}

func StartScanQueue() {
	logger.Log.Info("开始扫描所有延时队列")
	for name, tq := range Queues {
		logger.Log.Info("开始扫描延时队列-[" + name + "]")

		for bucket := 0; bucket < tq.bucketNum; bucket++ {
			go func(bkt int) {
				for range time.Tick(time.Millisecond) {
					tq.ScanBucket(bkt)
				}
			}(bucket)
		}
	}
	select {}
}

func (tq *TimeQueue) ScanBucket(bucket int) {
	elem, isGet := tq.CheckTimeout(bucket)
	if !isGet {
		return
	}

	logger.Log.Info("key: ", elem.GetKey(), "过期推送", elem.GetTime().Format("2006-01-02 15:04:05"), ", value: ", elem.GetValue())
	err := tq.Handler.HandleExpireData(elem.GetKey(), elem.GetValue(), 0)
	if err != nil {
		logger.Log.Error("处理推送失败", err)
	}
}

func (tq *TimeQueue) CheckTimeout(bucket int) (Element, bool) {
	if tq.Size(bucket) == 0 {
		return nil, false
	}
	now := time.Now()
	get, e := tq.PopTimeout(bucket, now)
	if !get {
		return nil, false
	}
	return e, true
}

func (tq *TimeQueue) GetAllQueues() ([]Element, int) {
	queueList := make([]Element, 0)
	queueSize := 0
	for bucket := 0; bucket < tq.bucketNum; bucket++ {
		queues := tq.GetList(bucket)
		queueList = append(queueList, queues...)

		queueSize += tq.Size(bucket)
	}
	return queueList, queueSize
}

func (tq *TimeQueue) HandleExpireData(key string, data interface{}, age int) error {
	logger.Log.Info("基类处理", key, data, age)
	return nil
}
