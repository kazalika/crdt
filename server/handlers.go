package crdt

import (
	"log"
	"net/http"
)

func (s *Server) PatchHandler(w http.ResponseWriter, r *http.Request) {
	if s.Stopped {
		return
	}
	// log.Println(s.ID, "Patch")
	set := decodeSet(r)
	res := s.BroadcastFromLocal(*set)
	log.Println(s.ID, "Server.PatchHandler result:", res)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) SyncHandler(w http.ResponseWriter, r *http.Request) {
	if s.Stopped {
		return
	}
	// log.Println(s.ID, "Sync")
	msg := decodeMessage(r)
	s.Sync(msg)
	log.Println(s.ID, "Server.SyncHandler done")
	w.WriteHeader(http.StatusOK)
}

func (s *Server) PatchFromNodeHandler(w http.ResponseWriter, r *http.Request) {
	if s.Stopped {
		return
	}
	// log.Println(s.ID, "Patch from node")
	msg := decodeMessage(r)
	res := s.BroadcastFromNode(msg)
	log.Println(s.ID, "Server.PatchFromNodeHandler result:", res)
	w.WriteHeader(http.StatusOK)
}
