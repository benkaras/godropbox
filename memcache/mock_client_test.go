package memcache

import (
	"bytes"
	"context"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
)

type MockClientSuite struct {
	client ContextClient
}

var _ = Suite(&MockClientSuite{})

func (s *MockClientSuite) SetUpTest(c *C) {
	s.client = NewMockContextClient()
}

func (s *MockClientSuite) TestAddSimple(c *C) {
	item := createTestItem()

	resp := s.client.Add(context.Background(), item)
	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Key(), Equals, item.Key)
	c.Assert(resp.DataVersionId(), Equals, uint64(1))

	gresp := s.client.Get(context.Background(), item.Key)
	c.Assert(gresp.Error(), IsNil)
	c.Assert(bytes.Equal(gresp.Value(), item.Value), IsTrue)
	c.Assert(gresp.DataVersionId(), Equals, uint64(1))
}

func (s *MockClientSuite) TestAddExists(c *C) {
	item := createTestItem()

	s.client.Add(context.Background(), item)
	resp := s.client.Add(context.Background(), item)
	c.Assert(resp.Error(), Not(IsNil))
	c.Assert(resp.Status(), Equals, StatusItemNotStored)
}

func (s *MockClientSuite) TestAddMultiSimple(c *C) {
	item1 := createTestItem()
	item2 := createTestItem()
	item2.Key = "foo"
	items := []*Item{item1, item2}

	resps := s.client.AddMulti(context.Background(), items)

	c.Assert(resps, HasLen, 2)
	for i := 0; i < 2; i++ {
		item := items[i]
		resp := resps[i]

		c.Assert(resp.Error(), IsNil)
		c.Assert(resp.Key(), Equals, item.Key)
		c.Assert(resp.DataVersionId(), Equals, uint64(1+i))

		gresp := s.client.Get(context.Background(), item.Key)
		c.Assert(gresp.Error(), IsNil)
		c.Assert(bytes.Equal(gresp.Value(), item.Value), IsTrue)
		c.Assert(gresp.DataVersionId(), Equals, uint64(1+i))
	}
}

func (s *MockClientSuite) TestAddMultiEmpty(c *C) {
	items := make([]*Item, 0)
	resps := s.client.AddMulti(context.Background(), items)
	c.Assert(resps, HasLen, 0)
}
