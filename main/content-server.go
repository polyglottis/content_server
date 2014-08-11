// Package main contains the content-server executable.
package main

import (
	"flag"
	"log"
	"path/filepath"

	"github.com/polyglottis/content_server/database"
	"github.com/polyglottis/content_server/operations"
	"github.com/polyglottis/content_server/server"
	"github.com/polyglottis/rpc"
)

var dbFile = flag.String("db", "content.db", "path to sqlite db file")
var tcpAddr = flag.String("tcp", ":18982", "TCP address of content server")
var operationsAddr = flag.String("op-tcp", ":16485", "TCP address of operations RPC server")

func main() {
	flag.Parse()

	abs, err := filepath.Abs(*dbFile)
	if err != nil {
		log.Fatalln(err)
	}

	db, err := database.Open(abs)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("Content server accessing db file %v", abs)

	main := server.New(server.NewServerDB(db), *tcpAddr)
	op := operations.NewOpServer(db, *operationsAddr)
	p := rpc.NewServerPair("Content Server", main, op)

	err = p.RegisterAndListen()
	if err != nil {
		log.Fatalln(err)
	}
	defer p.Close()

	p.Accept()
}
