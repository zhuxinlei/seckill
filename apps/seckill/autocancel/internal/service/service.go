package service

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/dtm-labs/driver-gozero"
	goredislib "github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/zeromicro/go-zero/zrpc"
	"github.com/zhoushuguang/lebron/apps/order/rpc/order"
	"github.com/zhoushuguang/lebron/apps/product/rpc/product"
	"github.com/zhoushuguang/lebron/apps/seckill/autocancel/internal/config"
	"log"
	"sync"
	"time"
)

const (
	chanCount   = 10
	bufferCount = 1024
)

type Service struct {
	c          config.Config
	ProductRPC product.Product
	OrderRPC   order.Order

	waiter   sync.WaitGroup
	msgsChan []chan *KafkaData
	test     chan *KafkaData
}

type KafkaData struct {
	Oid string `json:"oid"`
}

var n int
var start int64
var end int64

func NewService(c config.Config) *Service {
	s := &Service{
		c:          c,
		ProductRPC: product.NewProduct(zrpc.MustNewClient(c.ProductRPC)),
		OrderRPC:   order.NewOrder(zrpc.MustNewClient(c.OrderRPC)),
		msgsChan:   make([]chan *KafkaData, chanCount),
	}
	// 服务启动就开始了10个goroutine等待消费消息，
	//msgsChan 是一个初始长度为10的slice,元素是channel,buffer长度是1024，
	//这里的buffer长度是指传递过来的队列里的数据，也就是每次购买不同种类商品的数量
	for i := 0; i < chanCount; i++ {

		//这里限制的是用户一次性下单买多少个商品的数量，超过1024就慢慢的处理
		ch := make(chan *KafkaData, bufferCount)

		// 这里其实是非常简单的，先开辟内存，使用make,在使用<- 进行赋值的操作，只不过有点绕
		s.msgsChan[i] = ch
		//s.msgsChan[i] <- &KafkaData{}
		s.waiter.Add(1)

		//实现秒杀，但没使用分布式事务，极端情况下可能存在多个微服务之间数据不一致问题
		//go s.consume(ch)

		//使用分布式事务，但没有做超卖控制，也就是控制了数据读取
		//go s.consumeDTM(ch)

		//实现了超卖控制和分布式事务
		//go s.consumeSecAndDtm(ch)

		go s.autoCancelOrder(ch)

		//订单15分钟不消费，自动取消
	}

	return s
}

func (s *Service) autoCancelOrder(ch chan *KafkaData) {
	for {
		m, ok := <-ch
		if !ok {
			log.Fatal("seckill rmq exit")
		}

		fmt.Printf("mq data : %+v\n", m)
		go func() {
			timer := time.NewTicker(1 * time.Minute)
			//定时1分钟之后修改订单的状态
			<- timer.C
			s.OrderRPC.UpdateOrderStatus(context.Background(),&order.UpdateOrderStatusRequest{
				Oid: m.Oid,
			})
		}()



	}
}

func (s *Service) Consume(_ string, value string) error {
	//logx.Infof("Consume value: %s\n", value)

	var data []*KafkaData

	/*if start == 0{

		start = time.Now().UnixMilli()
		fmt.Println("stsrt---",start)
	}*/

	if err := json.Unmarshal([]byte(value), &data); err != nil {
		return err
	}
	n = n + len(data)
	/*if n > 10 {
		end = time.Now().UnixMilli()
		fmt.Println(start)
		fmt.Println(end)
		//os.Exit(0)
	}*/

	for _, d := range data {
		// d是什么类型，是*KafkaData类型，
		//s.msgsChan[d.Pid%chanCount]是什么？很明显是channel
		//这里是典型的先make开辟空间，在用<-进行赋值
		s.msgsChan[1] <- d
	}
	return nil
}

//使用分布式锁实现资源锁定
func (s *Service) consumeMutex(ch chan *KafkaData) {
	defer s.waiter.Done()
	for {
		m, ok := <-ch
		if !ok {
			log.Fatal("seckill rmq exit")
		}
		fmt.Printf("consume mutex msg: %+v\n", m)

		//
		client := goredislib.NewClient(&goredislib.Options{
			Addr: "localhost:6379",
		})

		pool := goredis.NewPool(client)
		rs := redsync.New(pool)
		mutextname := "my-global-mutex"
		mutex := rs.NewMutex(mutextname)

		if err := mutex.Lock(); err != nil {
			fmt.Println(err.Error())
		}
		defer mutex.Unlock()

		//分布式
		_, err := s.ProductRPC.CheckProductStock(context.Background(), &product.UpdateProductStockRequest{
			ProductId: 1,
			Num:       1,
		})
		if err != nil {
			continue
		}



	}
}


//var dtmServer = "etcd://localhost:2379/dtmservice"
var dtmServer = "localhost:36790"

