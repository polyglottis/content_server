package operations

import (
	"github.com/polyglottis/content_server/database"
	"github.com/polyglottis/rpc"
)

type OpRpcServer struct {
	db *database.DB
}

func NewOpServer(db *database.DB, addr string) *rpc.Server {
	return rpc.NewServer("OpRpcServer", &OpRpcServer{db}, addr)
}

func (s *OpRpcServer) DoNothing(nothing bool, nothing_too *bool) error {
	return nil
}
