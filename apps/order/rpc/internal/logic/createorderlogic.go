package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/zhoushuguang/lebron/apps/order/rpc/internal/svc"
	"github.com/zhoushuguang/lebron/apps/order/rpc/order"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CreateOrderLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateOrderLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateOrderLogic {
	return &CreateOrderLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CreateOrderLogic) CreateOrder(in *order.CreateOrderRequest) (*order.CreateOrderResponse, error) {
	oid := genOrderID(time.Now())
	err := l.svcCtx.OrderModel.CreateOrder(l.ctx, oid, in.Uid, in.Pid)
	if err != nil {
		logx.Errorf("OrderModel.CreateOrder oid: %s uid: %d pid: %d", oid, in.Uid, in.Pid)
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	//发送消息到kafka,15分钟没有响应的话，自动取消该订单
	type temp1  struct{
		Oid string `json:"oid"`
	}
	temp2 := temp1{Oid: oid}
	var msgs []temp1
	msgs = append(msgs,temp2)

	temp3,_ := json.Marshal(msgs)
	_ = l.svcCtx.KafkaPusher.Push(string(temp3))


	return &order.CreateOrderResponse{}, nil
}

var num int64

func genOrderID(t time.Time) string {
	s := t.Format("20060102150405")
	m := t.UnixNano()/1e6 - t.UnixNano()/1e9*1e3
	ms := sup(m, 3)
	p := os.Getpid() % 1000
	ps := sup(int64(p), 3)
	i := atomic.AddInt64(&num, 1)
	r := i % 10000
	rs := sup(r, 4)
	n := fmt.Sprintf("%s%s%s%s", s, ms, ps, rs)
	return n
}

func sup(i int64, n int) string {
	m := fmt.Sprintf("%d", i)
	for len(m) < n {
		m = fmt.Sprintf("0%s", m)
	}
	return m
}
