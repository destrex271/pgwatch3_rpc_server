package main

import (
	"fmt"
	"log"
	"net/rpc"
)

type Args struct{
    Name string
    Age int16
}

func main(){

    args := Args{}
    args.Name = "A"
    args.Age = 10

    client, err := rpc.DialHTTP("tcp", "localhost"+":1234")

    if err != nil{
        log.Fatal("error:", err)
    }

    reply := 12 
    err = client.Call("Server.UpdateMetrics", &args, &reply)
    if err != nil{
        log.Fatal(err)
    }
    fmt.Println(reply)
}
