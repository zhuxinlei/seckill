package service

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/dtm-labs/driver-gozero"
	"github.com/dtm-labs/dtmcli/logger"
	"github.com/dtm-labs/dtmgrpc"
	goredislib "github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
	"github.com/zhoushuguang/lebron/apps/order/rpc/order"
	"github.com/zhoushuguang/lebron/apps/product/rpc/product"
	"github.com/zhoushuguang/lebron/apps/seckill/rmq/internal/config"
	"log"
	"sync"
)

const (
	chanCount   = 1
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
	Uid int64 `json:"uid"`
	Pid int64 `json:"pid"`
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
		go s.consumeSecAndDtm(ch)

		//go s.consumeMutex(ch)

		//订单15分钟不消费，自动取消
	}

	return s
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
		s.msgsChan[d.Pid%chanCount] <- d
	}
	return nil
}


func (s *Service)autoCancelOrder()  {

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
		_,err := s.ProductRPC.CheckProductStock(context.Background(),&product.UpdateProductStockRequest{
			ProductId: 1,
			Num:       1,
		})
		if err != nil{
			continue
		}



		s.consumeDTMOnce(m)

	}
}


func (s *Service) consume(ch chan *KafkaData) {
	defer s.waiter.Done()

	for {
		m, ok := <-ch
		if !ok {
			log.Fatal("seckill rmq exit")
		}
		fmt.Printf("consume native msg: %+v\n", m)
		_, err := s.ProductRPC.CheckAndUpdateStock(context.Background(), &product.CheckAndUpdateStockRequest{ProductId: m.Pid})
		if err != nil {
			logx.Errorf("s.ProductRPC.CheckAndUpdateStock pid: %d error: %v", m.Pid, err)
			//return 这里用return是错误的
			continue
		}
		//走到这一步的数据量一定非常小，所以可以直接操作mysql
		_, err = s.OrderRPC.CreateOrder(context.Background(), &order.CreateOrderRequest{Uid: m.Uid, Pid: m.Pid})
		if err != nil {
			logx.Errorf("CreateOrder uid: %d pid: %d error: %v", m.Uid, m.Pid, err)
		}

		_, err = s.ProductRPC.UpdateProductStock(context.Background(), &product.UpdateProductStockRequest{ProductId: m.Pid, Num: 1})
		if err != nil {
			logx.Errorf("UpdateProductStock uid: %d pid: %d error: %v", m.Uid, m.Pid, err)
		}
	}
}

func (s *Service) consumeSecAndDtm(ch chan *KafkaData) {
	defer s.waiter.Done()

	for {
		m, ok := <-ch
		if !ok {
			log.Fatal("seckill rmq exit")
		}
		fmt.Printf("consume native msg: %+v\n", m)
		_, err := s.ProductRPC.CheckAndUpdateStock(context.Background(), &product.CheckAndUpdateStockRequest{ProductId: m.Pid})
		if err != nil {
			logx.Errorf("s.ProductRPC.CheckAndUpdateStock pid: %d error: %v", m.Pid, err)
			//return 这里用return是错误的
			continue
		}
		//走到这一步的数据量一定非常小，所以可以直接操作mysql
		s.consumeDTMOnce(m)
		fmt.Println("正常消费完成")
	}
}

//var dtmServer = "etcd://localhost:2379/dtmservice"
var dtmServer = "localhost:36790"

func (s *Service) consumeDTM(ch chan *KafkaData) {

	defer s.waiter.Done()

	productServer, err := s.c.ProductRPC.BuildTarget()

	if err != nil {
		log.Fatalf("s.c.ProductRPC.BuildTarget error: %v", err)
	}
	orderServer, err := s.c.OrderRPC.BuildTarget()
	if err != nil {
		log.Fatalf("s.c.OrderRPC.BuildTarget error: %v", err)
	}

	productServer = "127.0.0.1:9002"
	orderServer = "127.0.0.1:9003"
	//等待队列中的数据
	for {
		m, ok := <-ch
		if !ok {
			log.Fatal("seckill rmq exit")
		}
		fmt.Printf("consume dtm msg: %+v\n", m)

		gid := dtmgrpc.MustGenGid(dtmServer)

		err := dtmgrpc.TccGlobalTransaction(dtmServer, gid, func(tcc *dtmgrpc.TccGrpc) error {
			if e := tcc.CallBranch(
				&product.UpdateProductStockRequest{ProductId: m.Pid, Num: 1},
				productServer+"/product.Product/CheckProductStock",
				productServer+"/product.Product/UpdateProductStock",
				productServer+"/product.Product/RollbackProductStock",
				&product.UpdateProductStockRequest{},
			); e != nil {
				fmt.Println("tcc product error")
				fmt.Println(m.Uid)
				fmt.Println(e.Error())

				logx.Errorf("tcc.CallBranch server: %s error: %v", productServer, e.Error())

				return nil
			}
			if e := tcc.CallBranch(
				&order.CreateOrderRequest{Uid: m.Uid, Pid: m.Pid},
				orderServer+"/order.Order/CreateOrderCheck",
				orderServer+"/order.Order/CreateOrder",
				orderServer+"/order.Order/RollbackOrder",
				&order.CreateOrderResponse{},
			); e != nil {
				fmt.Println("tcc order error")
				logx.Errorf("tcc.CallBranch server: %s error: %v", orderServer, err)

				return nil
			}
			return nil
		})
		logger.FatalIfError(err)
	}

}


func (s *Service) consumeDTMOnce(m *KafkaData) {

	productServer, err := s.c.ProductRPC.BuildTarget()

	if err != nil {
		log.Fatalf("s.c.ProductRPC.BuildTarget error: %v", err)
	}
	orderServer, err := s.c.OrderRPC.BuildTarget()
	if err != nil {
		log.Fatalf("s.c.OrderRPC.BuildTarget error: %v", err)
	}

	productServer = "127.0.0.1:9002"
	orderServer = "127.0.0.1:9003"
	//等待队列中的数据

	fmt.Printf("consume dtm msg: %+v\n", m)

	gid := dtmgrpc.MustGenGid(dtmServer)

	err = dtmgrpc.TccGlobalTransaction(dtmServer, gid, func(tcc *dtmgrpc.TccGrpc) error {
		if e := tcc.CallBranch(
			&product.UpdateProductStockRequest{ProductId: m.Pid, Num: 1},
			productServer+"/product.Product/CheckProductStock",
			productServer+"/product.Product/UpdateProductStock",
			productServer+"/product.Product/RollbackProductStock",
			&product.UpdateProductStockRequest{},
		); e != nil {
			fmt.Println("tcc product error")
			fmt.Println(m.Uid)
			fmt.Println(e.Error())

			logx.Errorf("tcc.CallBranch server: %s error: %v", productServer, e.Error())

			return nil
		}
		if e := tcc.CallBranch(
			&order.CreateOrderRequest{Uid: m.Uid, Pid: m.Pid},
			orderServer+"/order.Order/CreateOrderCheck",
			orderServer+"/order.Order/CreateOrder",
			orderServer+"/order.Order/RollbackOrder",
			&order.CreateOrderResponse{},
		); e != nil {
			fmt.Println("tcc order error")
			logx.Errorf("tcc.CallBranch server: %s error: %v", orderServer, err)

			return nil
		}
		return nil
	})

	logger.FatalIfError(err)

}
