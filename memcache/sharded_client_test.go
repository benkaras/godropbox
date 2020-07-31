package memcache

import (
	"context"
	"errors"

	. "gopkg.in/check.v1"

	"github.com/dropbox/godropbox/net2"
)

// MockShardManager
type MockShardManager struct {
	ShardManager
	*C

	shardMap map[int]*ShardMapping
}

func (m *MockShardManager) GetShardsForSentinelsFromItems(items []*Item) map[int]*ShardMapping {
	m.Assert(items, HasLen, 1)
	return m.shardMap
}

// BadMemcacheConn fails all write operations.
type BadMemcacheConn struct {
	net2.ManagedConn
}

func (BadMemcacheConn) Write([]byte) (int, error) {
	return 0, errors.New("Bad conn")
}

func (BadMemcacheConn) DiscardConnection() error {
	return nil
}

func (BadMemcacheConn) SetDeadlineFromContext(ctx context.Context) error {
	return nil
}

// ShardedContextClientSuite
type ShardedContextClientSuite struct {
	sm *MockShardManager
	mc ContextClient
}

var _ = Suite(&ShardedContextClientSuite{})

func (s *ShardedContextClientSuite) SetUpTest(c *C) {
	s.sm = &MockShardManager{C: c}
	s.mc = NewContextShardedClient(s.sm, NewContextRawBinaryClient)
}

type ShardedClientSuite struct {
	ShardedContextClientSuite
}

var _ = Suite(&ShardedClientSuite{})

func (s *ShardedClientSuite) SetUpTest(c *C) {
	s.sm = &MockShardManager{C: c}
	s.mc = newIgnoreContextClientAdapter(NewShardedClient(s.sm, NewRawBinaryClient))
}

func (s *ShardedContextClientSuite) TestSetSentinels(c *C) {
	keys := []string{"key"}
	items := []*Item{
		{
			Key:   "key",
			Value: []byte("value"),
		},
	}

	// In DOWN state, SetSentinels should always succeed.
	s.sm.shardMap = map[int]*ShardMapping{
		0: {
			ConnErr:    nil,
			Connection: nil,
			Keys:       keys,
			Items:      items,
			WarmingUp:  false,
		},
	}

	response := s.mc.SetSentinels(context.Background(), items)
	c.Assert(response, HasLen, 1)
	c.Assert(response[0].Error(), IsNil)
	c.Assert(response[0].DataVersionId(), Equals, uint64(0))

	// In WARM_UP state, SetSentinels should succeed with bad connection.
	s.sm.shardMap = map[int]*ShardMapping{
		0: {
			ConnErr:    nil,
			Connection: BadMemcacheConn{},
			Keys:       keys,
			Items:      items,
			WarmingUp:  true,
		},
	}

	response = s.mc.SetSentinels(context.Background(), items)
	c.Assert(response, HasLen, 1)
	c.Assert(response[0].Error(), IsNil)
	c.Assert(response[0].DataVersionId(), Equals, uint64(0))

	// In WRITE_ONLY & ACTIVE state, SetSentinels should fail with bad connection.
	s.sm.shardMap = map[int]*ShardMapping{
		0: {
			ConnErr:    nil,
			Connection: BadMemcacheConn{},
			Items:      items,
			WarmingUp:  false,
		},
	}

	response = s.mc.SetSentinels(context.Background(), items)
	c.Assert(response, HasLen, 1)
	c.Assert(response[0].Error(), NotNil)
}
