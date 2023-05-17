package logic

import (
	"context"
	"fmt"

	"github.com/zhoushuguang/lebron/apps/product/rpc/internal/svc"
	"github.com/zhoushuguang/lebron/apps/product/rpc/product"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CheckAndUpdateStockLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCheckAndUpdateStockLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CheckAndUpdateStockLogic {
	return &CheckAndUpdateStockLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

//--------------------------------------------------import--------------------------------------------------
//执行之前先执行命令
//hmset stock:1 total 10 seckill 0
//将第一个商品的总量设置成10，秒杀数量初始化为0
//--------------------------------------------------import--------------------------------------------------
const (
	luaCheckAndUpdateScript = `
redis.log(redis.LOG_NOTICE, "hello world")
local counts = redis.call("HMGET", KEYS[1], "total", "seckill")
local total = tonumber(counts[1])
local seckill = tonumber(counts[2])
if  total >= 1  then
	redis.call("HINCRBY", KEYS[1], "seckill", 1)
	redis.call("HINCRBY", KEYS[1], "total", -1)
	return 1
end
return 0
`
)

func (l *CheckAndUpdateStockLogic) CheckAndUpdateStock(in *product.CheckAndUpdateStockRequest) (*product.CheckAndUpdateStockResponse, error) {
	val, err := l.svcCtx.BizRedis.EvalCtx(l.ctx, luaCheckAndUpdateScript, []string{stockKey(in.ProductId)})
	if err != nil{
		return nil, err
	}
	if val.(int64) == 0 {
		return nil, status.Errorf(codes.ResourceExhausted, fmt.Sprintf("insufficient stock: %d", in.ProductId))
	}
	return &product.CheckAndUpdateStockResponse{}, nil
}

func stockKey(pid int64) string {
	return fmt.Sprintf("stock:%d", pid)
}
