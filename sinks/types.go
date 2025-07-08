package sinks

import (
	"context"
	"fmt"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SyncMetricHandler struct {
	syncChannel chan *pb.SyncReq
	pb.UnimplementedReceiverServer
}

func NewSyncMetricHandler(chanSize int) SyncMetricHandler {
	if chanSize == 0 {
		chanSize = 1024
	}
	return SyncMetricHandler{syncChannel: make(chan *pb.SyncReq, chanSize)}
}

func (handler *SyncMetricHandler) SyncMetric(ctx context.Context, req *pb.SyncReq) (*pb.Reply, error) {
	if req.GetOperation() != pb.SyncOp_AddOp && req.GetOperation() != pb.SyncOp_DeleteOp {
		return nil, status.Errorf(codes.InvalidArgument, "invalid operation type")
	}
	if req.GetDBName() == "" && req.GetMetricName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid sync request both DBName and MetricName are empty")
	}

	opName := "Add"
	if req.GetOperation() == pb.SyncOp_DeleteOp {
		opName = "Delete"
	}

	select {
	case handler.syncChannel <- req:
		reply := &pb.Reply{
			Logmsg: fmt.Sprintf("gRPC Receiver Synced: DBName %s MetricName %s Operation %s", req.GetDBName(), req.GetMetricName(), opName),
		}
		return reply, nil
	case <-time.After(5 * time.Second):
		return nil, status.Errorf(codes.DeadlineExceeded, "timeout while trying to sync metric")
	}
}

func (handler *SyncMetricHandler) GetSyncChannelContent() (*pb.SyncReq, bool) {
	content, ok := <-handler.syncChannel
	return content, ok
}

func (handler *SyncMetricHandler) HandleSyncMetric() {
	for {
		// default HandleSyncMetric = empty channel and do nothing
		handler.GetSyncChannelContent()
	}
}