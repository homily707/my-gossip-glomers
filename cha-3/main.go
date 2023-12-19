package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	s := &server{node: n}
	n.Handle("broadcast", s.broadcast)
	n.Handle("read", s.read)
	n.Handle("topology", s.topology)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	node *maelstrom.Node
	sync.RWMutex
	ids []int
}

func (s *server) broadcast(msg maelstrom.Message) error {
	body, err := parseBodyIntoMap(msg.Body)
	if err != nil {
		return err
	}
	s.Lock()
	s.ids = append(s.ids, int(body["message"].(float64)))
	s.Unlock()
	return s.node.Reply(msg, map[string]interface{}{
		"type": "broadcast_ok",
	})
}

func (s *server) read(msg maelstrom.Message) error {
	s.RLock()
	ids := make([]int, len(s.ids))
	for i := 0; i < len(s.ids); i++ {
		ids[i] = s.ids[i]
	}
	s.RUnlock()
	return s.node.Reply(msg, map[string]interface{}{
		"type":     "read_ok",
		"messages": ids,
	})
}

func (s *server) topology(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]interface{}{
		"type":     "topology_ok",
	})
}

func parseBodyIntoMap(body json.RawMessage) (map[string]interface{}, error) {
	var bodyMap map[string]interface{}
	if err := json.Unmarshal(body, &bodyMap); err != nil {
		return nil, err
	}
	return bodyMap, nil
}
