package crdt

import (
	"flag"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
)

type Server struct {
	Mux                   *chi.Mux
	ID                    string
	Peers                 []string
	ReceivedMsgCounters   map[Key]VectorClock
	ReceivedCounterForMsg map[Key]map[MessageID]int
	CurCounter            map[Key]int
	Received              map[Key][]*LilMessage

	OnlineMode  bool
	SyncTimeout time.Duration

	State Set

	Stopped bool

	Mu sync.Mutex
}

type Key string

type Value struct {
	Str       string
	IsDeleted bool
}

type VectorClock map[string]int

func (v VectorClock) isLess(o VectorClock) bool {
	leq := false
	geq := false

	for k, val := range v {
		if val <= o[k] {
			leq = true
		}
		if val >= o[k] {
			geq = true
		}
	}

	if leq != geq {
		return leq
	}
	return false
}

type Element struct {
	Key   Key   `json:"key"`
	Value Value `json:"value"`
}

type Set struct {
	Set []Element `json:"elements"`
}

type MessageID struct {
	Creator string `json:"creator"`
	ID      int    `json:"id"`
}

type Message struct {
	IDs       map[Key]MessageID   `json:"message_ids"`
	Set       *Set                `json:"set"`
	VecClocks map[Key]VectorClock `json:"vector_clocks"`
}

type LilMessage struct {
	ID       MessageID
	El       Element
	VecClock VectorClock
}

func NewServer(id string, peers []string) *Server {
	s := &Server{
		Mux:                   chi.NewRouter(),
		ID:                    id,
		Peers:                 peers,
		ReceivedMsgCounters:   make(map[Key]VectorClock),
		ReceivedCounterForMsg: make(map[Key]map[MessageID]int),
		CurCounter:            make(map[Key]int),
		OnlineMode:            true,
		SyncTimeout:           2 * time.Second,
		State:                 Set{Set: []Element{}},
		Received:              make(map[Key][]*LilMessage),
		Mu:                    sync.Mutex{},
	}

	s.Mux.Post("/patch", s.PatchHandler)
	s.Mux.Post("/node/patch", s.PatchFromNodeHandler)
	s.Mux.Post("/node/sync", s.SyncHandler)

	return s
}

func (s *Server) BroadcastFromLocal(set Set) bool {
	log.Println(s.ID[15:], "Broadcast from local", set)
	msg := Message{
		IDs:       make(map[Key]MessageID),
		Set:       &set,
		VecClocks: make(map[Key]VectorClock),
	}

	for _, el := range set.Set {

		s.Mu.Lock()

		if _, ok := s.ReceivedMsgCounters[el.Key]; !ok {
			s.ReceivedMsgCounters[el.Key] = make(VectorClock)
			s.ReceivedCounterForMsg[el.Key] = make(map[MessageID]int)
			for _, peer := range s.Peers {
				s.ReceivedMsgCounters[el.Key][peer] = 0
			}
		}
		if _, ok := s.CurCounter[el.Key]; !ok {
			s.CurCounter[el.Key] = 0
		}

		s.CurCounter[el.Key]++
		msg.IDs[el.Key] = MessageID{
			ID:      s.CurCounter[el.Key],
			Creator: s.ID,
		}

		msg.VecClocks[el.Key] = make(VectorClock)
		for k, v := range s.ReceivedMsgCounters[el.Key] {
			msg.VecClocks[el.Key][k] = v
		}
		msg.VecClocks[el.Key][s.ID] = s.CurCounter[el.Key]

		if _, ok := s.ReceivedCounterForMsg[el.Key]; !ok {
			s.ReceivedCounterForMsg[el.Key] = make(map[MessageID]int)
		}
		s.ReceivedCounterForMsg[el.Key][msg.IDs[el.Key]] = 1

		s.Mu.Unlock()

	}

	for _, peer := range s.Peers {
		if peer != s.ID {
			go sendMessage(peer+"/node/patch", &msg)
		}
	}

	return true
}

func (s *Server) BroadcastFromNode(msg *Message) bool {
	log.Println(s.ID[15:], "Broadcast from node", string(encodeMessage(msg)))
	msgToBroadcast := Message{
		IDs: make(map[Key]MessageID),
		Set: &Set{
			Set: []Element{},
		},
		VecClocks: make(map[Key]VectorClock),
	}
	for _, el := range msg.Set.Set {
		if s.GetMessage(&LilMessage{
			ID:       msg.IDs[el.Key],
			El:       el,
			VecClock: msg.VecClocks[el.Key],
		}) {
			log.Println("Get message returned true for el = ", el)

			msgToBroadcast.Set.Set = append(msgToBroadcast.Set.Set, el)
			msgToBroadcast.IDs[el.Key] = msg.IDs[el.Key]
			msgToBroadcast.VecClocks[el.Key] = msg.VecClocks[el.Key]
		}
	}
	if len(msgToBroadcast.Set.Set) != 0 {
		for _, peer := range s.Peers {
			if peer != s.ID {
				go sendMessage(peer+"/node/patch", &msgToBroadcast)
			}
		}
		return true
	}
	return false
}

func (s *Server) CheckMessage(msg *LilMessage) bool {

	s.Mu.Lock()

	bad := false
	if msg.ID.ID-s.ReceivedMsgCounters[msg.El.Key][msg.ID.Creator] != 1 {
		bad = true
	}
	for _, peer := range s.Peers {
		if peer != msg.ID.Creator && s.ReceivedMsgCounters[msg.El.Key][peer] < msg.VecClock[peer] {
			bad = true
			break
		}
	}

	log.Println(s.ID[15:], "Server.CheckMessage: bad = ", bad, "for msg = ", msg.ID)
	if bad {
		s.Received[msg.El.Key] = append(s.Received[msg.El.Key], msg)

		s.Mu.Unlock()
		return false
	}

	s.ReceivedMsgCounters[msg.El.Key][msg.ID.Creator]++
	delete(s.ReceivedCounterForMsg[msg.El.Key], msg.ID)
	s.Apply(msg)

	s.Mu.Unlock()

	return true
}

func (s *Server) CheckMessages(key Key) {

	s.Mu.Lock()

	msgs := s.Received[key]
	s.Received[key] = make([]*LilMessage, 0)

	s.Mu.Unlock()

	sort.Slice(msgs, func(i, j int) bool {
		vci := msgs[i].VecClock
		vcj := msgs[j].VecClock
		if vci.isLess(vcj) {
			return true
		}
		if vcj.isLess(vci) {
			return false
		}
		return msgs[i].ID.Creator < msgs[j].ID.Creator
	})

	for _, msg := range msgs {
		s.CheckMessage(msg)
	}
}

func (s *Server) GetMessage(msg *LilMessage) bool {

	s.Mu.Lock()

	if _, ok := s.ReceivedMsgCounters[msg.El.Key]; !ok {
		s.ReceivedMsgCounters[msg.El.Key] = make(VectorClock)
		s.ReceivedCounterForMsg[msg.El.Key] = make(map[MessageID]int)
		for _, peer := range s.Peers {
			s.ReceivedMsgCounters[msg.El.Key][peer] = 0
		}
	}
	log.Println(s.ID[15:], "Server.GetMessage", msg.ID, s.ReceivedMsgCounters[msg.El.Key])
	if s.ReceivedMsgCounters[msg.El.Key][msg.ID.Creator] >= msg.ID.ID {

		s.Mu.Unlock()
		return false
	}

	isNewMsg := false
	if _, ok := s.ReceivedCounterForMsg[msg.El.Key][msg.ID]; !ok {
		s.ReceivedCounterForMsg[msg.El.Key][msg.ID] = 1
		isNewMsg = true
	}

	if s.ReceivedCounterForMsg[msg.El.Key][msg.ID] > len(s.Peers)/2 {

		s.Mu.Unlock()
		return isNewMsg
	}
	s.ReceivedCounterForMsg[msg.El.Key][msg.ID]++
	log.Println(s.ID[15:], "Message counter updated!", msg.ID, s.ReceivedCounterForMsg[msg.El.Key][msg.ID])
	if s.ReceivedCounterForMsg[msg.El.Key][msg.ID] <= len(s.Peers)/2 {

		s.Mu.Unlock()
		return isNewMsg
	}

	s.Mu.Unlock()

	if s.CheckMessage(msg) {
		s.CheckMessages(msg.El.Key)
	}

	return isNewMsg
}

// With Lock
func (s *Server) Apply(msg *LilMessage) {
	log.Println(s.ID[15:], "Apply lil message", msg.ID, msg.El, msg.VecClock)
	i := 0
	for i < len(s.State.Set) && s.State.Set[i].Key != msg.El.Key {
		i++
	}
	if i != len(s.State.Set) {
		s.State.Set[i].Value = msg.El.Value
	} else {
		s.State.Set = append(s.State.Set, msg.El)
	}
	s.ReceivedMsgCounters[msg.El.Key] = msg.VecClock

}

func (s *Server) Sync(msg *Message) {

	s.Mu.Lock()

	log.Println(s.ID[15:], "Server.Sync", s.ReceivedMsgCounters, msg.VecClocks)
	for _, el := range msg.Set.Set {
		if s.ReceivedMsgCounters[el.Key].isLess(msg.VecClocks[el.Key]) {
			s.Received[el.Key] = make([]*LilMessage, 0)
			s.ReceivedCounterForMsg[el.Key] = make(map[MessageID]int)
			s.Apply(&LilMessage{
				ID:       msg.IDs[el.Key],
				El:       el,
				VecClock: msg.VecClocks[el.Key],
			})
		} else if !msg.VecClocks[el.Key].isLess(s.ReceivedMsgCounters[el.Key]) && msg.IDs[el.Key].Creator < s.ID {
			s.Received[el.Key] = make([]*LilMessage, 0)
			s.ReceivedCounterForMsg[el.Key] = make(map[MessageID]int)
			s.Apply(&LilMessage{
				ID:       msg.IDs[el.Key],
				El:       el,
				VecClock: msg.VecClocks[el.Key],
			})
		}
	}

	s.Mu.Unlock()

}

func (s *Server) Start() {
	go func() {
		log.Println("Start server", s.ID)
		log.Fatal(http.ListenAndServe(s.ID[7:], s.Mux))
	}()

	go func() {
		for {
			time.Sleep(s.SyncTimeout)

			s.Mu.Lock()

			msg := Message{
				IDs:       make(map[Key]MessageID),
				Set:       &s.State,
				VecClocks: s.ReceivedMsgCounters,
			}
			for _, el := range msg.Set.Set {
				msg.IDs[el.Key] = MessageID{
					ID:      s.CurCounter[el.Key],
					Creator: s.ID,
				}
			}

			s.Mu.Unlock()

			if !s.Stopped {
				log.Println(s.ID[15:], "Initiate sync!", msg.IDs, msg.Set.Set)
				for _, peer := range s.Peers {
					sendMessage(peer+"/node/sync", &msg)
				}
			}
		}
	}()
}

func (s *Server) Stop() {
	s.Stopped = true
}

func (s *Server) Continue() {
	s.Stopped = false
}

func main() {
	var addr string
	flag.StringVar(&addr, "addr", "localhost:5252", "Replica address")
	flag.Parse()
	s := NewServer("http://"+addr, []string{"http://0.0.0.0:5252", "http://0.0.0.0:5253", "http://0.0.0.0:5254"})
	s.Start()

	for {
	} // wait fatal
}
