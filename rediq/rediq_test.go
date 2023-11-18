package rediq

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"gosuc/go-timequeue/logger"
	"gosuc/go-timequeue/utils"
)

// BenchmarkMutilScan 测试部分 ============================================================================
func BenchmarkMutilScan(b *testing.B) {
	b.StopTimer()
	logger.Init("debug")

	redisOption := &utils.RedisConfig{
		Host:     "172.25.119.44",
		Port:     6379,
		Password: "",
		DB:       10,
	}
	err := InitRediQs(24, redisOption)
	if err != nil {
		panic(err)
	} // 记载所有队列

	userQ := Use("user_push")
	userQ.Handler = &testExpireHandler{}

	totalCnt := 0
	go func() {
		for range time.Tick(1 * time.Second) {
			b.Log("当前扫描总数:", totalCnt)
		}
	}()

	b.StartTimer()

	for _, rq := range rediQs.QueueList {
		var wg sync.WaitGroup
		// 启动扫描桶
		for _, bucketName := range rq.buckets {
			bucket, err := strconv.Atoi(bucketName)
			if err != nil {
				b.Error("扫描桶失败,桶名转换失败")
				panic(err)
			}
			wg.Add(1)

			go func(bkt int) {
				defer wg.Done()
				cnt := rq.ScanBucket(bkt)
				totalCnt += cnt
			}(bucket)
		}

		wg.Wait()
	}

}

type testExpireHandler struct{}

func (handler *testExpireHandler) HandleExpireData(data RediQElementCommon) error {
	logger.Log.Info("UserPushExpireHandler - key:", data.Key, " data:", data.Data, " age:", data.Age)
	return nil
}
