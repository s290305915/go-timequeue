package main

import (
	"context"
	"flag"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"time"

	"gosuc/go-timequeue/logger"
	"gosuc/go-timequeue/push"
	"gosuc/go-timequeue/rediq"
	"gosuc/go-timequeue/router"
	"gosuc/go-timequeue/schedule"
	"gosuc/go-timequeue/utils"
)

const (
	pprofAddr string = ":7890"
)

func StartHTTPDebuger() {
	pprofHandler := http.NewServeMux()
	pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	server := &http.Server{Addr: pprofAddr, Handler: pprofHandler}
	go server.ListenAndServe()
}

var runFlag int

func init() {
	// 接受flag参数
	flag.IntVar(&runFlag, "run_flag", 1, "启动参数:\n 1 使用cn_timequeue.go方式\n 2 使用schedule.go方式")
}

func main() {
	flag.Parse()

	go StartHTTPDebuger()

	logger.Init("debug")

	notifyUlr := "http://127.0.0.1:8080/push"         // 通知回调地址
	bucketNum := 24                                   // 定时任务桶数量
	scd := schedule.NewSchedule(bucketNum, notifyUlr) // 初始化定时任务器

	if runFlag == 1 {
		// 方式一，使用链表，16核i9-12900k，并发3-5w的时候，cpu占用率在70%左右
		// 并发10w，cpu很快上100，导致程序挂掉
		// 并发4w，cpu占用率在30%-100%左右，内存基本无开销
		// 消费速度，1w+-/s

		redisOption := &utils.RedisConfig{
			Host:     "172.25.119.44",
			Port:     6379,
			Password: "",
			DB:       10,
		}
		err := rediq.InitRediQs(bucketNum, redisOption)
		if err != nil {
			panic(err)
		} // 记载所有队列

		go push.InitAllPushChannel() // 初始化所有推送通道
	} else {
		go scd.Start() // 开始扫描所有桶并推送到期数据
		// 方式二，使用map，16核i9-12900k，并发2w的时候，cpu占用率在30%左右
		// 并发高于2w,直接cpu上100，导致程序挂掉
		// 消费速度（存量数据越少消费越快），平均<1w/s且有重复处理问题，cpu25%左右，内存基本无开销
	}

	listenPort := ":8080"

	logger.Log.Info("启动服务[", listenPort, "]")

	initRouters := router.InitRouters()

	srv := &http.Server{
		Addr:    listenPort,
		Handler: initRouters,
	}

	go func() {
		// 启动http服务器
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log.Error("listen: %s\n", err)
		}
	}()

	// 等待中断信号以优雅地关闭服务器（设置 5 秒的超时时间）
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	//scd.CloseChan <- true

	logger.Log.Info("正在停止 ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Log.Error("服务停止失败:", err)
	}
	logger.Log.Info("服务结束")
}
