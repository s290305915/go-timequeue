package rediq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/spaolacci/murmur3"
	"strconv"
	"sync"
	"time"

	"gosuc/go-timequeue/logger"
	"gosuc/go-timequeue/utils"
)

var rediQs *RediQs // 时间队列，内部使用常量

type RediQs struct {
	Redis     *redis.Client     // Redis客户端实例
	bucketNum int               // 桶数量
	ctx       context.Context   // 上下文
	QueueList map[string]*RediQ // 队列
	rootKey   string            // 根键名 list 存储所有队列名
}

type RediQ struct {
	QueueName string     // 队列名称
	lock      sync.Mutex // 并发锁
	buckets   []string   // 当前队列所有桶 hash key为桶名，value为桶key
	// 桶内数据结构，使用双向链表，方便循环处理和移除，key为桶名，value为桶内数据sortedset

	Handler RediExpireDataHandler // 自定义过期数据处理器
}

// RediQElement 队列元素接口，用于实现自定义队列元素
type RediQElement interface {
	GetKey() string
	GetValue() interface{}
	GetTime() time.Time
}

type RediQElementCommon struct {
	Key      string
	Data     any
	Age      int
	OverTime time.Time
}

func (r *RediQElementCommon) GetKey() string {
	return r.Key
}
func (r *RediQElementCommon) GetData() interface{} {
	return r.Data
}
func (r *RediQElementCommon) GetAge() int {
	return r.Age
}
func (r *RediQElementCommon) GetTime() time.Time {
	return r.OverTime
}

// RediExpireDataHandler 自定义过期数据处理器接口，用于实现自定义过期数据处理器
type RediExpireDataHandler interface {
	HandleExpireData(elem RediQElementCommon) error
}

func InitRediQs(bucketNum int, redisConf *utils.RedisConfig) error {
	rediQs = &RediQs{
		bucketNum: bucketNum,
		rootKey:   "rediQs",
		ctx:       context.Background(),
		QueueList: make(map[string]*RediQ),
	}

	// 初始化redis客户端
	redisOpt := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", redisConf.Host, redisConf.Port), // Redis 服务器地址和端口
		Password: redisConf.Password,                                   // Redis 认证密码，如果没有设置密码则为空字符串
		DB:       redisConf.DB,                                         // Redis 数据库索引
	}

	redisClient := redis.NewClient(redisOpt)
	pingRes, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		return err
	}
	logger.Log.Infof("连接redis：%s", pingRes)

	rediQs.Redis = redisClient

	// 加载已存在的队列
	// 获取所有队列名
	queueNames, err := rediQs.Redis.HGetAll(rediQs.ctx, rediQs.rootKey).Result()
	logger.Log.Infof("加载已存在的队列%+v", queueNames)
	if err != nil {
		return err
	}

	// 加载队列
	for qName, _ := range queueNames {
		_, err := newRediQ(qName)
		if err != nil {
			return err
		}
	}
	return nil
}

func newRediQ(queueName string) (*RediQ, error) {
	rediQ := &RediQ{
		QueueName: queueName,
		lock:      sync.Mutex{},
		Handler:   &RediQ{}, // 仅仅用于handler占位，防止未实现时候的报错
	}
	// 初始化队列
	var qBuckets []string
	for i := 0; i < rediQs.bucketNum; i++ {
		// 预先分配桶
		qBucketKey := strconv.Itoa(i)
		qBuckets = append(qBuckets, qBucketKey)
	}
	rediQs.Redis.HSet(rediQs.ctx, queueName, qBuckets) // 存放 *list.Element 推送数据的链表指针
	rediQ.buckets = qBuckets

	rediQs.Redis.HSetNX(rediQs.ctx, rediQs.rootKey, queueName, queueName)

	rediQs.QueueList[queueName] = rediQ
	return rediQ, nil
}

func Use(queueName string) *RediQ {
	if _, ok := rediQs.QueueList[queueName]; !ok {
		rdq, err := newRediQ(queueName)
		if err != nil {
			logger.Log.Error(err)
			panic(err)
		}
		rediQs.QueueList[queueName] = rdq
	}

	return rediQs.QueueList[queueName]
}

// 基础数据结构处理部分 ============================================================================

func (brq *RediQ) exists(bucket int, key string) bool {
	setKey := brq.QueueName + ":" + strconv.Itoa(bucket)

	// 从 Redis 获取元素
	qVal, err := rediQs.Redis.ZScore(rediQs.ctx, setKey, key).Result()
	if err != nil {
		logger.Log.Error("Failed to get element from Redis list:", err)
		return false
	}
	if qVal == 0 {
		return false
	}
	return true
}

func (brq *RediQ) push(bucket int, e RediQElementCommon) bool {
	brq.lock.Lock()
	defer brq.lock.Unlock()

	setKey := brq.QueueName + ":" + strconv.Itoa(bucket)

	// 将元素转化为 JSON 格式
	jsonData, err := json.Marshal(e)
	if err != nil {
		logger.Log.Error("Failed to marshal element:", err)
		return false
	}

	pipeline := rediQs.Redis.TxPipeline()
	// 将元素添加到 Redis 排序集合中
	pipeline.ZAdd(rediQs.ctx, setKey, &redis.Z{
		Score:  float64(e.GetTime().Unix()),
		Member: e.GetKey(),
	})
	tick := e.GetTime().Sub(time.Now()) * 2
	pipeline.Set(rediQs.ctx, e.GetKey(), jsonData, tick)

	// 有写入，延长队列过期时间
	pipeline.Expire(rediQs.ctx, brq.QueueName, 24*time.Hour)

	_, err = pipeline.Exec(rediQs.ctx)
	if err != nil {
		logger.Log.Error("Failed to push element to Redis list:", err)
		return false
	}
	return true
}

func (brq *RediQ) cancel(bucket int, key string) bool {
	brq.lock.Lock()
	defer brq.lock.Unlock()

	setKey := brq.QueueName + ":" + strconv.Itoa(bucket)

	// 删除 Redis 排序集合中的元素
	pipeline := rediQs.Redis.TxPipeline()
	pipeline.ZRem(rediQs.ctx, setKey, key)
	pipeline.Del(rediQs.ctx, key)
	_, err := pipeline.Exec(rediQs.ctx)
	if err != nil {
		logger.Log.Error("Failed to remove element from Redis sorted set:", err)
		return false
	}

	return true
}

func (brq *RediQ) size(bucket int) int64 {
	setKey := brq.QueueName + ":" + strconv.Itoa(bucket)

	// 获取 Redis 集合中的元素数量
	qLen, _ := rediQs.Redis.ZCard(rediQs.ctx, setKey).Result()
	return qLen
}

func (brq *RediQ) getList(bucket int) []RediQElementCommon {
	setKey := brq.QueueName + ":" + strconv.Itoa(bucket)

	qList, err := rediQs.Redis.ZRange(rediQs.ctx, setKey, 0, -1).Result()
	if err != nil {
		logger.Log.Error("Failed to get elements from Redis sorted set:", err)
		return nil
	}

	vList := rediQs.Redis.MGet(rediQs.ctx, qList...).Val()

	var data []RediQElementCommon
	// 遍历排序集合中的所有元素
	for _, qVal := range vList {
		if qVal == nil {
			continue
		}

		var e RediQElementCommon
		qValStr := qVal.(string)
		err := json.Unmarshal([]byte(qValStr), &e)
		if err != nil {
			return nil
		}
		data = append(data, e)
	}
	return data
}

func (brq *RediQ) walk(bucket int, cb func(e RediQElementCommon)) {
	setKey := brq.QueueName + ":" + strconv.Itoa(bucket)

	// 使用 ZSCAN 命令迭代有序集合中的所有元素
	var cursor uint64 = 0
	for {
		keys, newCur, err := rediQs.Redis.ZScan(rediQs.ctx, setKey, cursor, "", 1).Result()
		if err != nil {
			logger.Log.Error("Failed to scan Redis sorted set:", err)
			return
		}
		cursor = newCur

		if len(keys) == 0 {
			return
		}

		// 遍历并处理每个元素
		key := keys[0]

		qVal, _ := rediQs.Redis.Get(rediQs.ctx, key).Result()

		var e RediQElementCommon
		err = json.Unmarshal([]byte(qVal), &e)
		if err != nil {
			return
		}
		cb(e)
	}

}

func (brq *RediQ) popTimeout(bucket int) (isGet bool, res []RediQElementCommon) {
	setKey := brq.QueueName + ":" + strconv.Itoa(bucket)
	// 使用 ZSCAN 命令迭代有序集合
	for {
		// 获取当前时间戳的 Unix 表示
		nowUnix := time.Now().Unix()

		// 使用 ZRANGEBYSCORE 命令获取所有到期的元素
		cmd := rediQs.Redis.ZRangeByScore(rediQs.ctx, setKey, &redis.ZRangeBy{
			Min: "-inf",
			Max: strconv.FormatInt(nowUnix, 10),
		})
		keys, err := cmd.Result()
		if err != nil {
			logger.Log.Error("Failed to get expired elements from Redis sorted set:", err)
			return false, nil
		}

		// 如果没有到期的元素，则返回空数组
		if len(keys) == 0 {
			return false, nil
		}

		// mget获取所有到期数据
		results := rediQs.Redis.MGet(rediQs.ctx, keys...)

		// 解析元素的 JSON 数据并返回数组
		var expiredElements []RediQElementCommon
		for _, result := range results.Val() {
			if result == nil {
				continue
			}

			value := result.(string)

			var e RediQElementCommon
			err = json.Unmarshal([]byte(value), &e)
			if err != nil {
				logger.Log.Error("Failed to unmarshal element:", value, "    ", err)
				continue
			}

			expiredElements = append(expiredElements, e)
		}

		// 使用 Pipeline 批量获取元素值和删除元素
		pipeline := rediQs.Redis.TxPipeline()
		for _, key := range keys {
			pipeline.Del(rediQs.ctx, key)
			pipeline.ZRem(rediQs.ctx, setKey, key)
		}
		_, err = pipeline.Exec(rediQs.ctx)
		if err != nil {
			logger.Log.Error("Failed to retrieve and remove expired elements from Redis:", err)
			return false, nil
		}

		return true, expiredElements
	}
}

func hash(key string) ([]uint64, int) {
	// 做哈希
	runeArr := []rune(key)
	hashes := make([]uint64, len(runeArr))

	bucket := murmur3.Sum32([]byte(key)) % uint32(rediQs.bucketNum)

	for i, r := range runeArr {
		d := uint64(r)
		hashes[i] = d
	}

	return hashes, int(bucket)
}

// 逻辑处理部分 ============================================================================

func (brq *RediQ) PushRediQueue(key string, age int, data interface{}) error {
	_, bucket := hash(key)

	pushData := RediQElementCommon{
		Key:      key,
		Data:     data,
		Age:      age,
		OverTime: time.Now().Add(time.Duration(age) * time.Second),
	}

	toTimeStr := pushData.OverTime.Format("2006-01-02 15:04:05")

	logger.Log.Info("["+key+"]写入，过期时间：", toTimeStr, "，数据：", data)

	brq.push(bucket, pushData)

	return nil
}

func (brq *RediQ) CancelQueue(key string) bool {
	_, bucket := hash(key)
	isOk := brq.cancel(bucket, key)
	return isOk
}

func (brq *RediQ) ScanBucket(bucket int) int {
	if brq.size(bucket) == 0 {
		//logger.Log.Info("bucket: ", bucket, "没有数据")
		return 0
	}

	isGet, elems := brq.popTimeout(bucket)
	if !isGet {
		return 0
	}

	for _, elem := range elems {
		logger.Log.Info("key: ", elem.GetKey(), "过期推送", elem.GetTime().Format("2006-01-02 15:04:05"), ", value: ", elem.GetData())
		err := brq.Handler.HandleExpireData(elem)
		if err != nil {
			logger.Log.Error("处理推送失败", err)
		}
	}

	return len(elems)
}

func (brq *RediQ) GetAllQueues() ([]RediQElementCommon, int64) {
	queueList := make([]RediQElementCommon, 0)
	var queueSize int64 = 0
	for _, bucket := range brq.buckets {
		intBucket, _ := strconv.Atoi(bucket)

		queues := brq.getList(intBucket)
		queueList = append(queueList, queues...)

		queueSize += brq.size(intBucket)
	}
	return queueList, queueSize
}

func (brq *RediQ) GetAllSize() int64 {
	var queueSize int64 = 0
	for _, bucket := range brq.buckets {
		intBucket, _ := strconv.Atoi(bucket)
		queueSize += brq.size(intBucket)
	}
	return queueSize
}

func (brq *RediQ) HandleExpireData(elem RediQElementCommon) error {
	logger.Log.Info("基类处理", elem.Key, elem.Data, elem.Age)
	panic("no implement error!")
	return nil
}

func StartScanRediQueues() {
	logger.Log.Info("开始扫描所有延时队列")
	for range time.Tick(100 * time.Millisecond) {
		for _, rq := range rediQs.QueueList {
			//logger.Log.Info("开始扫描延时队列-[" + name + "]")

			var wg sync.WaitGroup
			// 启动扫描桶
			for _, bucketName := range rq.buckets {
				bucket, err := strconv.Atoi(bucketName)
				if err != nil {
					logger.Log.Error("扫描桶失败,桶名转换失败")
					panic(err)
				}
				wg.Add(1)

				//rq.ScanBucket(bucket)

				go func(bkt int) {
					defer wg.Done()
					rq.ScanBucket(bkt)
				}(bucket)
			}

			wg.Wait()
		}
	}
}
