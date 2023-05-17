package logic

import (
	"context"

	"github.com/zhoushuguang/lebron/apps/order/rpc/internal/svc"
	"github.com/zhoushuguang/lebron/apps/order/rpc/order"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateOrderStatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateOrderStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateOrderStatusLogic {
	return &UpdateOrderStatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateOrderStatusLogic) UpdateOrderStatus(in *order.UpdateOrderStatusRequest) (*order.UpdateOrderStatusResponse, error) {
	// todo: add your logic here and delete this line
	l.svcCtx.OrderModel.UpdateOrderStatus(context.Background(),in.Oid,60)
	return &order.UpdateOrderStatusResponse{}, nil
}
