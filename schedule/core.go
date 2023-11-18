package schedule

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"gosuc/go-timequeue/logger"
	"gosuc/go-timequeue/utils"
)

var GlobalSchedule *Schedule

type Schedule struct {
	bucketNum     int            // 桶数量
	Buckets       sync.Map       // 对应 桶id : key	 ， 这里用redis list 如 0:[key1,key2,key3]
	TimingLists   sync.Map       // 对应 key : 过期时间	 ， 这里redis sortedset 如: {0:{key1:1234567}}
	PushData      sync.Map       // 对应 key : data	 ， 这里redis hash 如: {key1:data}
	wg            sync.WaitGroup // 并行执行扫描等待组
	notifyUrl     string         // 回调通知地址
	Publisher     *Publisher     // 发布者
	CloseChan     chan bool      // 关闭通道
	PublishLock   sync.Mutex     // 发布锁
	ProcessedKeys sync.Map       // 标记池
}

func NewSchedule(bucketNum int, notifyUrl string) *Schedule {
	GlobalSchedule = &Schedule{
		bucketNum:   bucketNum,
		Buckets:     sync.Map{},
		TimingLists: sync.Map{},
		PushData:    sync.Map{},
		wg:          sync.WaitGroup{},
		notifyUrl:   notifyUrl,
		Publisher: &Publisher{
			Buffer:      1024,
			Timeout:     100 * time.Second,
			Subscribers: make(map[Subscriber]TopicFunc),
		},
		CloseChan:     make(chan bool),
		PublishLock:   sync.Mutex{},
		ProcessedKeys: sync.Map{},
	}

	for i := 0; i < bucketNum; i++ {
		// 预先分配桶
		GlobalSchedule.Buckets.Store(i, []string{})
	}

	utils.NewBloom(bucketNum)

	return GlobalSchedule
}

func (s *Schedule) Start() {
	// 初始化后 异步调用此方法进行监听
	go s.scanSchedule() // 开始扫描所有桶

	for outKey := range s.Publisher.Subscriber() {
		// 有到期数据，进行异步推送
		key := outKey.(string)
		s.pushSchedule(key)
	}
}

func (s *Schedule) publishKey(bucket int, key string) {
	tgKey := fmt.Sprintf("%d_%s", bucket, key)
	tgTimeUnix, isOk := s.TimingLists.Load(tgKey)
	if !isOk {
		return
	}

	tgTime := time.Unix(tgTimeUnix.(int64), 0)
	nowTime := time.Now()

	if tgTime.Sub(nowTime) <= 0*time.Second {
		s.Publisher.Publish(key)
	}
}

func (s *Schedule) scanBucket(bucket int) {
	defer s.wg.Done()

	keyList, isOk := s.Buckets.Load(bucket)
	if !isOk || len(keyList.([]string)) <= 0 {
		//logger.Info("扫描桶[" + strconv.Itoa(bucket) + "]为空，等下次扫描：")
		return
	}

	keys := keyList.([]string)
	//logger.Info("扫描桶[" + strconv.Itoa(bucket) + "]剩余数量：" + strconv.Itoa(len(keys)) + "个")

	for _, key := range keys {
		// 检查是否已处理过
		_, processed := s.ProcessedKeys.LoadOrStore(key, 1)
		if processed {
			t := time.NewTimer(1 * time.Millisecond)
			<-t.C

			s.ProcessedKeys.Delete(key)
			continue
		}

		s.PublishLock.Lock()
		s.publishKey(bucket, key)
		s.PublishLock.Unlock()
	}
}

func (s *Schedule) scanSchedule() {
	logger.Log.Info("开始扫描")

	for {
		select {
		case <-s.CloseChan:
			return
		default:
			{
				for bucket := 0; bucket < s.bucketNum; bucket++ {
					s.wg.Add(1)
					go s.scanBucket(bucket)
				}

				s.wg.Wait()
			}
		}
	}
}

// 执行删除操作，包括当前处理的key 和 已经过期了但是没处理成功的key
func (s *Schedule) dropKey(key string) {
	BlObj := utils.GetHashBloom(key)
	isExt := utils.GlobalBloom.ExistsBloom(key)
	if !isExt {
		panic("[" + key + "]不存在于过滤器中！")
	}

	bucket := BlObj.Bucket

	tgKey := fmt.Sprintf("%d_%s", bucket, key)

	logger.Log.Info("["+key+"]开始清理", tgKey)

	s.TimingLists.Delete(tgKey)

	// 剔除bucket中当前key
	keyList, ok := s.Buckets.Load(bucket)
	if !ok {
		return
	}
	keys := keyList.([]string)

	// 找出要删除的元素在切片中的索引
	var idx = -1
	for i, x := range keys {
		if x == key {
			idx = i
			break
		}
	}

	// 如果元素存在，则从切片中删除
	if idx != -1 {
		newKeys := append(keys[:idx], keys[idx+1:]...)
		s.Buckets.Store(bucket, newKeys)
	}

	// 删除数据
	s.PushData.Delete(key)

	// 从布隆过滤器里面移除特征
	utils.GlobalBloom.RemoveBloom(key)

	// 从标记池中移除
	s.ProcessedKeys.Delete(key)

	logger.Log.Info("[" + key + "]清理完成")
}

func (s *Schedule) pushSchedule(key string) {
	logger.Log.Info("[" + key + "]即将过期，开始推送")
	data, isExts := s.PushData.Load(key)
	if !isExts {
		logger.Log.Warn("[" + key + "]推送数据不存在,请确认是否已推送过了")
		return // 不存在，直接返回
	}
	logger.Log.Info("["+key+"]推送数据：", data)
	s.dropKey(key)
}

func (s *Schedule) SetSchedule(key string, age int, data interface{}) error {
	toTime := time.Now().Add(time.Duration(age) * time.Second)

	toTimeStr := toTime.Format("2006-01-02 15:04:05")

	// 如果布隆过滤器不存在，才设置，否则跳过
	blm := utils.GetHashBloom(key)
	blm, err := utils.GlobalBloom.SetBloom(key)
	if err != nil {
		return err
	}

	// 写入到bucket
	keyList, ok := s.Buckets.Load(blm.Bucket)
	if !ok {
		return fmt.Errorf("bucket not found")
	}
	keys := keyList.([]string)
	keys = append(keys, key)

	s.Buckets.Store(blm.Bucket, keys)

	tgKey := fmt.Sprintf("%d_%s", blm.Bucket, key)

	// 写入到timinglist
	s.TimingLists.Store(tgKey, toTime.Unix())
	s.PushData.Store(key, data)

	logger.Log.Info("["+key+"]写入，过期时间：", toTimeStr, "，数据：", data)

	return nil
}

func (s *Schedule) GetAllSchedule() (
	buckets map[string][]string,
	timingList map[string]int64,
	pushData map[string]interface{},
) {

	buckets = make(map[string][]string)
	for i := 0; i < s.bucketNum; i++ {
		keyList, isOk := s.Buckets.Load(i)
		if !isOk || len(keyList.([]string)) <= 0 {
			continue
		}
		buckets[strconv.Itoa(i)] = keyList.([]string)
	}

	timingList = make(map[string]int64)
	pushData = make(map[string]interface{})
	for b, l := range buckets {
		for _, key := range l {
			tgKey := fmt.Sprintf("%s_%s", b, key)
			tgTime, isOk := s.TimingLists.Load(tgKey)
			if !isOk {
				continue
			}
			timingList[tgKey] = tgTime.(int64)

			data, isOk := s.PushData.Load(key)
			if !isOk {
				continue
			}
			pushData[key] = data
		}
	}

	return
}
