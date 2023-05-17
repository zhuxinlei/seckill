package logic

import (
	"context"

	"github.com/zhoushuguang/lebron/apps/product/rpc/internal/svc"
	"github.com/zhoushuguang/lebron/apps/product/rpc/product"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UpdateProductStockLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateProductStockLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateProductStockLogic {
	return &UpdateProductStockLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateProductStockLogic) UpdateProductStock(in *product.UpdateProductStockRequest) (*product.UpdateProductStockResponse, error) {
	err := l.svcCtx.ProductModel.UpdateProductStock(l.ctx, in.ProductId, in.Num)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &product.UpdateProductStockResponse{}, nil
}
