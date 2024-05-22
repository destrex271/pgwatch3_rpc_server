package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Args struct{
    Name string
    Age int16
}

type Server struct{}

func (m *Server) UpdateMetrics(args *Args, reply *int) error{
    file, err := os.Create("metrics.text")
    fmt.Println("Writing to file")
    if err != nil{
        log.Fatal(err)
    }

    file.WriteString(args.Name + " " + fmt.Sprint(args.Age))
    file.Sync()
    file.Close()

    *reply = 10

    return nil
}

func main(){
    server := new(Server) 
    rpc.Register(server)
    rpc.HandleHTTP()

    listener, err := net.Listen("tcp", ":1234")
    if err != nil{
        log.Fatal(err)
    }

    http.Serve(listener, nil)
}
