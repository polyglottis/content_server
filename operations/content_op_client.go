// Package operations contains an rpc client-server pair for maintenance operations on the content server.
package operations

import (
	"net/rpc"
)

type Client struct {
	c *rpc.Client
}

// NewClient creates an rpc client for maintenance operations on the content server.
func NewClient(addr string) (*Client, error) {
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{c: c}, nil
}
