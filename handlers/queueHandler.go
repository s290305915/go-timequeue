package handlers

import (
	"gosuc/go-timequeue/logger"
	"gosuc/go-timequeue/models"
	"gosuc/go-timequeue/rediq"
	"gosuc/go-timequeue/timequeue"
	"gosuc/go-timequeue/utils"

	"github.com/gin-gonic/gin"
)

func Index(c *gin.Context) {
	logger.Log.Info("请求")

	c.JSON(200, utils.Result{
		Status:  0,
		Message: "go-timequeue 定时推送系统",
		Data: map[string]interface{}{
			"1.用法":   "",
			"2.推送接口": "POST /put -d {key:123,age:1200,data:{aa:bb,cc:dd}}",
			"3.接收接口": "POST /push -d {key:123,age:1200,data:{aa:bb,cc:dd}}",
			"4.字段解释": []string{
				"key - 即定时推送的唯一键，若存在则更新对应时间并重置推送任务",
				"age - 即定时时间，单位秒",
				"data - 即定时需要接收的数据，通过字符串传入",
			},
			"5.原理描述": "本系统通过接收需要定时推送的数据，写入定时任务后，到时间进行推送，无须特别处理，使用http调用即可，若需将接收数据推送至消息中间件，请另修改源码",
			"6.注意事项": "本系统基于内存开发，未使用数据持久化功能",
		},
	})
}

func PutSchedule(c *gin.Context) {
	var body models.ScheduleQuery
	err := c.ShouldBindJSON(&body)
	if err != nil {
		c.JSON(422, utils.Result{Status: 442, Message: err.Error()})
		return
	}

	//logger.Info("body -> ", body)

	// 写入定时任务系统
	//err = schedule.GlobalSchedule.SetSchedule(body.Key, body.Age, body.Data)
	//err = timequeue.Use("user_push").PushQueue(body.Key, body.Age, body.Data)
	err = rediq.Use("user_push").PushRediQueue(body.Key, body.Age, body.Data)
	if err != nil {
		c.JSON(500, utils.Result{Status: 500, Message: err.Error()})
		return
	}

	c.JSON(200, utils.Result{Status: 0, Message: "写入成功", Data: nil})
}

func CancelSchedule(c *gin.Context) {
	var body models.ScheduleQuery
	err := c.ShouldBindJSON(&body)
	if err != nil {
		c.JSON(422, utils.Result{Status: 442, Message: err.Error()})
		return
	}

	//logger.Info("body -> ", body)

	// 写入定时任务系统
	isOk := timequeue.Use("user_push").CancelQueue(body.Key)

	c.JSON(200, utils.Result{Status: 0, Message: "取消成功", Data: isOk})
}

func GetAllSchedule(c *gin.Context) {
	// 查询定时配置
	//bkt, tl, data := schedule.GlobalSchedule.GetAllSchedule()

	//_, qul := timequeue.Use("user_push").GetAllQueues()

	//_, rql := rediq.Use("user_push").GetAllQueues()
	rql := rediq.Use("user_push").GetAllSize()

	c.JSON(200, utils.Result{Status: 0, Message: "ok", Data: map[string]interface{}{
		//"a_total_bukets":    len(bkt),
		//"a_total_queue_len": len(tl),
		//"a_total_data_len":  len(data),
		//"a_total_queue":     qul,
		//"buckets":     bkt,
		//"timing_list": tl,
		//"push_data":   data,
		// "queues": qus,
		"rql": rql,
		//"rql_data": rqlData,
	}})
}
