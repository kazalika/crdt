package crdt

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
)

func sendMessage(url string, msg *Message) {
	b, err := json.Marshal(*msg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("send message = ", string(b), " to ", url)
	http.Post(url, "application/json", bytes.NewReader(b))
}

func encodeMessage(msg *Message) []byte {
	b, err := json.Marshal(*msg)
	if err != nil {
		log.Fatal(err)
	}
	return b
}

func decodeMessage(r *http.Request) *Message {
	var m Message
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&m)
	if err != nil {
		log.Fatal(err)
	}
	return &m
}

func decodeSet(r *http.Request) *Set {
	var s Set
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&s)
	if err != nil {
		log.Fatal(err)
	}
	return &s
}
