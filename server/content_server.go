// Package server defines the Polyglottis Content Server.
package server

import (
	"log"
	"sync"

	"github.com/polyglottis/content_server/database"
	"github.com/polyglottis/platform/content"
	contentRpc "github.com/polyglottis/platform/content/rpc"
	"github.com/polyglottis/platform/user"
	"github.com/polyglottis/rpc"
)

// New creates the rpc user server, as required by polyglottis/content/rpc
func New(server *Server, addr string) *rpc.Server {
	return contentRpc.NewContentServer(server, addr)
}

func NewServerDB(db *database.DB) *Server {
	return &Server{
		DB: db,
		slugToId: &slugToId{
			shouldRebuild: true,
			Mutex:         new(sync.Mutex),
		},
	}
}

func NewServer(dbFile string) (*Server, error) {
	db, err := database.Open(dbFile)
	if err != nil {
		return nil, err
	}
	return NewServerDB(db), nil
}

type Server struct {
	*database.DB
	slugToId *slugToId
}

type slugToId struct {
	m             map[string]content.ExtractId
	shouldRebuild bool
	*sync.Mutex
}

func (s *Server) NewExtract(author user.Name, e *content.Extract) error {
	err := s.DB.NewExtract(author, e)
	if err == nil {
		s.slugToId.shouldRebuild = true
	}
	return err
}

func (s *Server) UpdateExtract(author user.Name, e *content.Extract) error {
	err := s.DB.UpdateExtract(author, e)
	if err == nil {
		s.slugToId.shouldRebuild = true
	}
	return err
}

func (s *Server) GetExtractId(slug string) (content.ExtractId, error) {
	var id content.ExtractId
	s.withSlugToId(func(m map[string]content.ExtractId) {
		id = m[slug]
	})
	if len(id) == 0 {
		return "", content.ErrNotFound
	} else {
		return id, nil
	}
}

func (s *Server) withSlugToId(f func(map[string]content.ExtractId)) {
	s.slugToId.Lock()
	defer s.slugToId.Unlock()
	if s.slugToId.shouldRebuild {
		m, err := s.SlugToIdMap()
		if err == nil {
			s.slugToId.m = m
			s.slugToId.shouldRebuild = false
		} else {
			log.Println("Error: could not rebuild slugToId map:", err)
		}
	}
	f(s.slugToId.m)
}
