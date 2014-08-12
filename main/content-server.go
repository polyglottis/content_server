// Package main contains the content-server executable.
package main

import (
	"log"

	"github.com/polyglottis/content_server/database"
	"github.com/polyglottis/content_server/operations"
	"github.com/polyglottis/content_server/server"
	"github.com/polyglottis/platform/config"
	"github.com/polyglottis/rpc"
)

func main() {

	c := config.Get()

	db, err := database.Open(c.ContentDB)
	if err != nil {
		log.Fatalln(err)
	}

	main := server.New(server.NewServerDB(db), c.Content)
	op := operations.NewOpServer(db, c.ContentOp)
	p := rpc.NewServerPair("Content Server", main, op)

	err = p.RegisterAndListen()
	if err != nil {
		log.Fatalln(err)
	}
	defer p.Close()

	p.Accept()
}
