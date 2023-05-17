package batcher

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

var ErrFull = errors.New("channel is full")

/**
所有实现了apply方法的的变量都是Option类型的接口


*/
type Option interface {
	apply(*options)
}

type options struct {
	size     int
	buffer   int
	worker   int
	interval time.Duration
}

func (o options) check() {
	if o.size <= 0 {
		o.size = 100
	}
	if o.buffer <= 0 {
		o.buffer = 100
	}
	if o.worker <= 0 {
		o.worker = 5
	}
	if o.interval <= 0 {
		o.interval = time.Second
	}
}

type funcOption struct {
	f func(*options)
}

/*
funcOption类型的结构体实现了apply，所以funcOption类型的结构体是Option类型的接口
该结构体的变量是一个函数
该方法的逻辑是执行结构体中的方法
*/
func (fo *funcOption) apply(o *options) {
	fo.f(o)
}

func newOption(f func(*options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

/*
该方法返回一个Option接口类型的变量，因为该接口必须实现apply方法，
又因为funcOption类型的结构体实现了apply方法，
所以我们要返回一个该类型的结构体变量，newOption方法返回的就是funcOption类型的结构体
如何实现newOption方法？
已知，要返回的是一个结构体，且该结构体的成员变量是一个函数，那怎么构造这个方法？
直接构造？好像毫无意义，因为你根本就不知道应该构造成什么逻辑的方法，所以应该是传入进来的，定制化的
所以想要实例化该结构体需要传入一个方法作为参数，并且将该参数作为结构体的成员变量
待统一执行该方法中的内容
这种写法有什么好处，如此的难以理解？且看这里的用处，用于给方法中的参数（结构体）进行修改赋值，难道
不可以直接在初始化的时候赋值吗？以后想要修改的时候也可以修改啊
可以，

*/
func WithSize(s int) Option {
	return newOption(func(o *options) {
		o.size = s
	})
}

func WithBuffer(b int) Option {
	return newOption(func(o *options) {
		o.buffer = b
	})
}

func WithWorker(w int) Option {
	return newOption(func(o *options) {
		o.worker = w
	})
}

func WithInterval(i time.Duration) Option {
	return newOption(func(o *options) {
		o.interval = i
	})
}

type msg struct {
	key string
	val interface{}
}

type Batcher struct {
	opts options

	Do       func(ctx context.Context, val map[string][]interface{})
	Sharding func(key string) int
	chans    []chan *msg
	wait     sync.WaitGroup
}

func New(opts ...Option) *Batcher {
	b := &Batcher{}
	for _, opt := range opts {
		opt.apply(&b.opts)
	}
	b.opts.check()
	//chans类型为切片

	b.chans = make([]chan *msg, b.opts.worker)
	for i := 0; i < b.opts.worker; i++ {
		/*创建一个channel,并设置缓冲区长度为100
		该channel盛放的的是要批量处理的数据，那思考下，为什么要用channel盛放？用其他结构存储可以吗？why?
		使用channel主要是利用了channel的缓冲区的非阻塞的作用，在缓冲区未满之前，可以继续往缓冲区发送数据，

		*/
		b.chans[i] = make(chan *msg, b.opts.buffer)
	}
	return b
}

func (b *Batcher) Start() {

	if b.Do == nil {
		log.Fatal("Batcher: Do func is nil")
	}
	if b.Sharding == nil {
		log.Fatal("Batcher: Sharding func is nil")
	}
	//b.wait.Add(len(b.chans))    好像没有用

	//
	for i, ch := range b.chans {
		go b.merge(i, ch)
	}

}

/*
重点在于对于调用者的值的修改（往channel中添加值），然后有一个channel实时监听
从这个channel中取值
那如何对值进行修改？首先调用方肯定是指针，剩下的就是语法问题了
*/
func (b *Batcher) Add(key string, val interface{}) error {
	ch, msg := b.add(key, val)
	select {
	//将用户发送的消息放入channel中，因为有别的地方一直在监听channel中是否有值，如果监听到有值
	//会立马将channel中的值取出进行处理
	//这里理解的重点在于channel的作用，在于goroutine之间的通信，这里是主goroutine和其他goroutine之间
	//的通信
	case ch <- msg:
	default:
		return ErrFull
	}
	return nil
}

//重点在于对
func (b *Batcher) add(key string, val interface{}) (chan *msg, *msg) {
	sharding := b.Sharding(key) % b.opts.worker
	ch := b.chans[sharding]
	msg := &msg{key: key, val: val}
	return ch, msg
}

func (b *Batcher) merge(idx int, ch <-chan *msg) {
	//defer b.wait.Done()

	var (
		msg        *msg
		count      int
		closed     bool
		lastTicker = true
		interval   = b.opts.interval
		vals       = make(map[string][]interface{}, b.opts.size)
	)
	if idx > 0 {
		//每个goroutine间隔不同的时间进行批量发送，以防止多个goroutine同时处理，造成拥堵的可能
		//interval 1秒
		// worker 10个goroutine
		interval = time.Duration(int64(idx) * (int64(b.opts.interval) / int64(b.opts.worker)))
	}

	ticker := time.NewTicker(interval)
	for {
		//
		select {
		//实时监听channel中是否有消息进来，如果有则进行处理
		case msg = <-ch:
			if msg == nil {
				closed = true
				break
			}
			count++
			vals[msg.key] = append(vals[msg.key], msg.val)
			if count >= b.opts.size {
				//break是跳出这个select，而没有跳出这个for循环
				break
			}
			continue //好像没啥用
			//首先，continue必须在for select中使用，不能单在select中使用
			//有用有用，只要执行一个case就会退出select，使用continue会跳到for
			//头部重新执行,也就是说如果map中的数量小于100个，则不会去执行逻辑判断
			//,如果大于100，会break,而break只会跳出select，去进行逻辑判断是否能进行批量执行逻辑
		case <-ticker.C:
			//好像没啥用，这个的作用只是设置了一个新的定时器
			//明白了，巧妙
			//因为
			if lastTicker {
				ticker.Stop()
				ticker = time.NewTicker(b.opts.interval)
				lastTicker = false
			}
		}
		if len(vals) > 0 {

			ctx := context.Background()
			//执行自定义方法
			b.Do(ctx, vals)

			//重置数据
			vals = make(map[string][]interface{}, b.opts.size)
			count = 0
		}
		if closed {
			ticker.Stop()
			return
		}
	}
}

/*func (b *Batcher) Close() {
	for _, ch := range b.chans {
		ch <- nil
	}
	b.wait.Wait()
}*/
