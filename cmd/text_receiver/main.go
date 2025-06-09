package main

import (
	"log"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
    "github.com/jessevdk/go-flags"
)

type CmdOpts struct {
	ServerOptions *sinks.Options `group:"Server Options"`
    StorageFolder string `short:"s" long:"storage-folder" description:"Folder to store data in" default:"./"`
}

func main() {
    var opts CmdOpts
    if _, err := flags.Parse(&opts); err != nil {
        return
    }

    server := NewTextReceiver(opts.StorageFolder)
    if err := sinks.Listen(server, opts.ServerOptions); err != nil {
        log.Fatal(err)
    }
}
