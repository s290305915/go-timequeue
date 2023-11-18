package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"
)

func BenchmarkMutilAdd(b *testing.B) {
	b.StopTimer()
	fmt.Println("启动测试进程")

	var wg sync.WaitGroup
	//maxConcurrency := 10000 // 最大并发数量
	//totalRequests := 100000 // 总请求数量

	maxConcurrency := b.N // 最大并发数量
	totalRequests := b.N  // 总请求数量

	counter := 0
	b.StartTimer()

	for i := 0; i < totalRequests; i++ {
		startTime := time.Now()
		wg.Add(1)

		//doMiter(i)

		go func(i int) {
			defer wg.Done()
			doMiter(i)
		}(i)

		if (i+1)%maxConcurrency == 0 {
			counter++
			wg.Wait()
			endTime := time.Now()
			costTime := endTime.Sub(startTime).Seconds()
			fmt.Println("当前并发次数:", counter, "总请求数量:", i, "耗时:", costTime, "秒")
			//time.Sleep(1 * time.Second)
		}
	}

	wg.Wait()

	fmt.Println("测试结束")
}

func doMiter(i int) {
	jsonData := map[string]interface{}{
		"key":  strconv.Itoa(i) + "_" + randomString(5),
		"age":  randomNumber(3),
		"data": randomString(20),
	}

	//fmt.Printf("发送数据:%+v \n", jsonData)

	jsonValue, _ := json.Marshal(jsonData)
	resp, err := http.Post("http://localhost:8080/put", "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		panic(err)
	}
	resp.Body.Close()

	time.Sleep(1 * time.Millisecond)
	//logger.Info("响应数据:", resp.Body)
	//return
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func randomNumber(max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max)
}
