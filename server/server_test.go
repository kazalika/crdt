package crdt_test

import (
	server "crdt"
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

type TestCluster struct {
	Nodes []*server.Server
}

var (
	port = 0
	mu   = sync.Mutex{}
)

func NewTestCluster(size int) *TestCluster {
	tc := &TestCluster{}

	peers := []string{}
	mu.Lock()
	for i := 0; i < size; i++ {
		peers = append(peers, "http://0.0.0.0:525"+strconv.Itoa(port))
		port++
	}
	mu.Unlock()

	for i := 0; i < size; i++ {
		tc.Nodes = append(tc.Nodes, server.NewServer(peers[i], peers))
	}

	return tc
}

func (tc *TestCluster) StartTestCluster() {
	for i := 0; i < len(tc.Nodes); i++ {
		tc.Nodes[i].Start()
	}
}

func (tc *TestCluster) Stop() {
	for i := 0; i < len(tc.Nodes); i++ {
		tc.Nodes[i].Stop()
	}
}

func (tc *TestCluster) Continue() {
	for i := 0; i < len(tc.Nodes); i++ {
		tc.Nodes[i].Continue()
	}
}

func TestBasic(t *testing.T) {
	// t.Parallel()
	cluster := NewTestCluster(3)
	cluster.StartTestCluster()

	_ = cluster.Nodes[0].BroadcastFromLocal(server.Set{
		Set: []server.Element{
			{
				Key: server.Key("KEY"),
				Value: server.Value{
					Str:       "VALUE",
					IsDeleted: false,
				},
			},
		},
	})

	time.Sleep(5 * time.Second)
	cluster.Stop()

	vecClock := server.VectorClock{
		cluster.Nodes[0].ID: 1,
		cluster.Nodes[1].ID: 0,
		cluster.Nodes[2].ID: 0,
	}

	for _, node := range cluster.Nodes {
		assert.EqualValues(t,
			server.Set{
				Set: []server.Element{
					{
						Key: server.Key("KEY"),
						Value: server.Value{
							Str:       "VALUE",
							IsDeleted: false,
						},
					},
				},
			},
			node.State,
		)
		assert.EqualValues(t,
			vecClock,
			node.ReceivedMsgCounters[server.Key("KEY")],
		)
	}
}

func TestConcurrentOperationsToDifferentKeys(t *testing.T) {
	// t.Parallel()
	cluster := NewTestCluster(3)
	cluster.StartTestCluster()

	for i, node := range cluster.Nodes {
		_ = node.BroadcastFromLocal(server.Set{
			Set: []server.Element{
				{
					Key: server.Key("KEY " + strconv.Itoa(i)),
					Value: server.Value{
						Str:       "VALUE " + strconv.Itoa(i),
						IsDeleted: false,
					},
				},
			},
		})
	}

	time.Sleep(5 * time.Second)
	cluster.Stop()
	for _, node := range cluster.Nodes {
		log.Println(node.ID)
		assert.EqualValues(t,
			3,
			len(node.State.Set),
		)
		assert.EqualValues(t,
			server.VectorClock{
				cluster.Nodes[0].ID: 1,
				cluster.Nodes[1].ID: 0,
				cluster.Nodes[2].ID: 0,
			},
			node.ReceivedMsgCounters[server.Key("KEY 0")],
		)
		assert.EqualValues(t,
			server.VectorClock{
				cluster.Nodes[0].ID: 0,
				cluster.Nodes[1].ID: 1,
				cluster.Nodes[2].ID: 0,
			},
			node.ReceivedMsgCounters[server.Key("KEY 1")],
		)
		assert.EqualValues(t,
			server.VectorClock{
				cluster.Nodes[0].ID: 0,
				cluster.Nodes[1].ID: 0,
				cluster.Nodes[2].ID: 1,
			},
			node.ReceivedMsgCounters[server.Key("KEY 2")],
		)
	}
}

func TestConcurrentOperationsToTheSameKey(t *testing.T) {
	// t.Parallel()
	cluster := NewTestCluster(3)
	cluster.StartTestCluster()
	cluster.Stop()

	for _, node := range cluster.Nodes {
		_ = node.BroadcastFromLocal(server.Set{
			Set: []server.Element{
				{
					Key: server.Key("KEY"),
					Value: server.Value{
						Str:       "VALUE",
						IsDeleted: false,
					},
				},
			},
		})
	}

	cluster.Continue()

	time.Sleep(5 * time.Second)
	cluster.Stop()
	for _, node := range cluster.Nodes {
		log.Println(node.ID)
		assert.EqualValues(t,
			1,
			len(node.State.Set),
		)

		sum := func(vc server.VectorClock) int {
			res := 0
			for _, v := range vc {
				res += v
			}
			return res
		}

		assert.EqualValues(t,
			1,
			sum(node.ReceivedMsgCounters[server.Key("KEY")]),
		)
	}
}

func TestRecovery(t *testing.T) {
	// t.Parallel()
	cluster := NewTestCluster(3)
	cluster.StartTestCluster()

	toStop := 1
	cluster.Nodes[toStop].Stop()

	for i, node := range cluster.Nodes {
		if i == toStop {
			continue
		}
		_ = node.BroadcastFromLocal(server.Set{
			Set: []server.Element{
				{
					Key: server.Key("KEY " + strconv.Itoa(i)),
					Value: server.Value{
						Str:       "VALUE " + strconv.Itoa(i),
						IsDeleted: false,
					},
				},
			},
		})
	}

	time.Sleep(5 * time.Second)

	assert.EqualValues(t,
		0,
		len(cluster.Nodes[toStop].State.Set),
	)
	for i, node := range cluster.Nodes {
		if i == toStop {
			continue
		}
		assert.EqualValues(t,
			2,
			len(node.State.Set),
		)
	}

	cluster.Nodes[toStop].Continue()

	time.Sleep(5 * time.Second)
	cluster.Stop()

	for _, node := range cluster.Nodes {
		assert.EqualValues(t,
			2,
			len(node.State.Set),
		)
	}
}

func TestStrongEventualConsistency(t *testing.T) {
	// t.Parallel()
	cluster := NewTestCluster(3)

	rand.Seed(uint64(time.Now().UnixNano()))
	var wg sync.WaitGroup

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node := cluster.Nodes[rand.Intn(len(cluster.Nodes))]
			patch := randomPatch()
			node.BroadcastFromLocal(patch)
		}()
	}
	wg.Wait()

	time.Sleep(10 * time.Second)
	cluster.Stop()

	for i := 1; i < len(cluster.Nodes); i++ {
		assert.Equal(t, cluster.Nodes[0].State, cluster.Nodes[i].State)
	}
}

func randomPatch() server.Set {
	key := fmt.Sprintf("key%d", rand.Intn(5))
	value := fmt.Sprintf("value%d", rand.Intn(1000))
	return server.Set{
		Set: []server.Element{
			{
				Key: server.Key("KEY " + key),
				Value: server.Value{
					Str:       "VALUE " + value,
					IsDeleted: false,
				},
			},
		},
	}
}
