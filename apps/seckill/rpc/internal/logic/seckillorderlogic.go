package logic

import (
	"context"
	"math/rand"
	"strconv"
	"time"

	"github.com/zhoushuguang/lebron/apps/product/rpc/product"
	"github.com/zhoushuguang/lebron/apps/seckill/rpc/internal/svc"
	"github.com/zhoushuguang/lebron/apps/seckill/rpc/seckill"
	"github.com/zhoushuguang/lebron/pkg/batcher"

	"github.com/zeromicro/go-zero/core/collection"
	"github.com/zeromicro/go-zero/core/limit"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SeckillOrderLogic struct {
	ctx        context.Context
	svcCtx     *svc.ServiceContext
	limiter    *limit.PeriodLimit
	localCache *collection.Cache
	batcher    *batcher.Batcher
	logx.Logger
}

type KafkaData struct {
	Uid int64  `json:"uid"`
	Pid int64  `json:"pid"`
	Key string `json:"key"`
}

const (
	limitPeriod       = 100    //时间
	limitQuota        = 1 //限流数
	seckillUserPrefix = "seckill#u#"
	localCacheExpire  = time.Second

	batcherSize     = 100
	batcherBuffer   = 100
	batcherWorker   = 10
	batcherInterval = time.Second*100
)

func NewSeckillOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext, batcher *batcher.Batcher) *SeckillOrderLogic {
	localCache, err := collection.NewCache(localCacheExpire)
	if err != nil {
		panic(err)
	}
	s := &SeckillOrderLogic{
		ctx:        ctx,
		svcCtx:     svcCtx,
		Logger:     logx.WithContext(ctx),
		localCache: localCache,
		limiter:    limit.NewPeriodLimit(limitPeriod, limitQuota, svcCtx.BizRedis, seckillUserPrefix),
		batcher:    batcher,
	}

	return s
}

//秒杀的入队操作非常简单，判断缓存中剩余数量是否>0,
func (l *SeckillOrderLogic) SeckillOrder(in *seckill.SeckillOrderRequest) (*seckill.SeckillOrderResponse, error) {
	//限流，通过Take方法第一个参数，这里使用有问题，不应该设置userid,设置成userid单个用户限流，无意义
	x := rand.Intn(50000000)
	in.UserId = int64(x)

	code, _ := l.limiter.Take(strconv.FormatInt(in.UserId, 10))

	if code == limit.OverQuota {
		return nil, status.Errorf(codes.OutOfRange, "Number of requests exceeded the limit")
	}
	p, err := l.svcCtx.ProductRPC.Product(l.ctx, &product.ProductItemRequest{ProductId: in.ProductId})
	if err != nil {
		return nil, err
	}
	if p.Stock <= 0 {
		return nil, status.Errorf(codes.OutOfRange, "Insufficient stock")
	}

	if err = l.batcher.Add(strconv.FormatInt(in.ProductId, 10), &KafkaData{Uid: in.UserId, Pid: in.ProductId}); err != nil {
		logx.Errorf("l.batcher.Add uid: %d pid: %d error: %v", in.UserId, in.ProductId, err)
	}

	return &seckill.SeckillOrderResponse{}, nil
}
