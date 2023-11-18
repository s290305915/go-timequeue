package schedule

import (
	"sync"
	"time"
)

type Subscriber chan interface{}        // 订阅者 是一个管道
type TopicFunc func(v interface{}) bool // 主题 过滤器函数

type Publisher struct {
	m           sync.RWMutex             // 互斥锁
	Buffer      int                      // 队列缓冲区大小
	Timeout     time.Duration            // 发布超时时间
	Subscribers map[Subscriber]TopicFunc // 所有订阅者消息
}

func (p *Publisher) SubscribeTopic(topic TopicFunc) chan interface{} {
	ch := make(chan interface{}, p.Buffer)
	p.m.Lock()
	p.Subscribers[ch] = topic
	p.m.Unlock()
	return ch
}

// Subscriber 订阅全部主题，没有过滤
func (p *Publisher) Subscriber() chan interface{} {
	return p.SubscribeTopic(nil)
}

// Evict 退出订阅
func (p *Publisher) Evict(sub chan interface{}) {
	p.m.Lock()
	defer p.m.Unlock()
	delete(p.Subscribers, sub)
	close(sub)
}

// 发送主题，可以容忍一定的超时
func (p *Publisher) sendTopic(sub Subscriber, topic TopicFunc, v interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	// 没有订阅该主题
	if topic != nil && !topic(v) {
		return
	}

	delay := time.NewTicker(p.Timeout)
	select {
	case sub <- v:
	case <-delay.C:
	}
}

// Publish 发布一个主题
func (p *Publisher) Publish(v interface{}) {
	p.m.RLock()
	defer p.m.RUnlock()

	var wg sync.WaitGroup
	for sub, topic := range p.Subscribers {
		wg.Add(1)
		go p.sendTopic(sub, topic, v, &wg)
	}
	wg.Wait()
}

// Close 关闭发布者对象，同时关闭所有的订阅者管道
func (p *Publisher) Close() {
	p.m.Lock()
	defer p.m.Unlock()
	for sub := range p.Subscribers {
		delete(p.Subscribers, sub)
		close(sub)
	}
}
