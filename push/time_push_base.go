package push

import (
	"gosuc/go-timequeue/rediq"
)

func InitAllPushChannel() {
	//queue := timequeue.NewTimeQueue("user_push", bucketNum)
	//queue.Handler = &UserPushExpireHandler{}
	//
	//// 启动扫描队列
	//timequeue.StartScanQueue()

	userQ := rediq.Use("user_push")
	userQ.Handler = &UserPushExpireHandler{}

	// 启动扫描队列
	rediq.StartScanRediQueues()
}
