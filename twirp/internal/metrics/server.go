package main

import (
	"context"
	"fmt"
	pb "main/rpc"
	"os"
	"time"

	"github.com/twitchtv/twirp"
)

// Server implements the Haberdasher service
type Server struct {}

func (s *Server) WriteMetrics(ctx context.Context, metric *pb.Metric) (status *pb.Status, err error) {
    file, err := os.Create("metrics" + fmt.Sprint(time.Now().Unix()) +".txt")
    if err != nil{
        return nil, twirp.Error(err.Error())
    }

    op, err := file.WriteString(metric.GetName() + " " + fmt.Sprint(metric.GetAge()))
    if err != nil{
        return nil, twirp.Error(err.Error())
    }

    status = new(pb.Status)
    status.Status = int32(op) 

    return status, nil
}

