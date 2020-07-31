package memcache

import (
	"context"
	"fmt"
	"time"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
)

// ContextRawAsciiClientSuite is a parameterized test suite. We want to share test methods
// for both the context-aware and non-context-aware clients. `check` names suites after their
// types, so we'll define methods on the base, and create subtypes for each instantiation.
type baseAsciiClientSuite struct {
	contextAware bool
	rw           *mockReadWriter
	client       ContextClientShard
}

type contextRawAsciiClientSuite struct {
	baseAsciiClientSuite
}

var _ = Suite(&contextRawAsciiClientSuite{})

func (s *contextRawAsciiClientSuite) SetUpTest(c *C) {
	s.contextAware = true
	s.rw = newMockReadWriter()
	s.client = NewContextRawAsciiClient(0, s.rw)
}

type rawAsciiClientSuite struct {
	baseAsciiClientSuite
}

var _ = Suite(&rawAsciiClientSuite{})

func (s *rawAsciiClientSuite) SetUpTest(c *C) {
	s.contextAware = false
	s.rw = newMockReadWriter()
	// NB: We'll trust that the adapter properly drops the 'context' parameter and calls
	// the underlying method on the raw ascii client.
	s.client = newIgnoreContextClientShardAdapter(NewRawAsciiClient(0, s.rw))
}

func (s *baseAsciiClientSuite) verifyContextAwareDeadline(ctx context.Context) error {
	if d, ok := ctx.Deadline(); ok && s.contextAware {
		if s.rw.deadline.IsZero() {
			return fmt.Errorf("conn timeout is default; want %s", time.Until(d))
		}
		if s.rw.deadline != d {
			return fmt.Errorf("conn timeout = %s; want <= %s", time.Until(s.rw.deadline), time.Until(d))
		}
	} else {
		if !s.rw.deadline.IsZero() {
			return fmt.Errorf("conn timeout = %s; want default", time.Until(s.rw.deadline))
		}
	}
	return nil
}

func (s *baseAsciiClientSuite) TestGet(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("VALUE key 333 4 12345\r\nitem\r\n")
	s.rw.recvBuf.WriteString("VALUE key2 42 6 14\r\nAB\r\nCD\r\n")
	s.rw.recvBuf.WriteString("END\r\n")

	responses := s.client.GetMulti(ctx, []string{"key2", "key"})

	c.Assert(s.rw.sendBuf.String(), Equals, "gets key2 key\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(len(responses), Equals, 2)

	resp, ok := responses["key"]
	c.Assert(ok, IsTrue)
	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Status(), Equals, StatusNoError)
	c.Assert(resp.Key(), Equals, "key")
	c.Assert(resp.Value(), DeepEquals, []byte("item"))
	c.Assert(resp.Flags(), Equals, uint32(333))
	c.Assert(resp.DataVersionId(), Equals, uint64(12345))

	resp, ok = responses["key2"]
	c.Assert(ok, IsTrue)
	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Status(), Equals, StatusNoError)
	c.Assert(resp.Key(), Equals, "key2")
	c.Assert(resp.Value(), DeepEquals, []byte("AB\r\nCD"))
	c.Assert(resp.Flags(), Equals, uint32(42))
	c.Assert(resp.DataVersionId(), Equals, uint64(14))
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestGetNotFound(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("END\r\n")

	resp := s.client.Get(ctx, "key")

	c.Assert(s.rw.sendBuf.String(), Equals, "gets key\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Status(), Equals, StatusKeyNotFound)
	c.Assert(resp.Key(), Equals, "key")
	c.Assert(resp.Value(), IsNil)
	c.Assert(resp.Flags(), Equals, uint32(0))
	c.Assert(resp.DataVersionId(), Equals, uint64(0))
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestGetDupKeys(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("END\r\n")

	_ = s.client.GetMulti(ctx, []string{"key", "key", "key2", "key"})

	c.Assert(s.rw.sendBuf.String(), Equals, "gets key key2\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestGetBadKey(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	resp := s.client.Get(ctx, "b a d")

	c.Assert(s.rw.sendBuf.String(), Equals, "")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
	c.Assert(s.rw.deadline, Equals, time.Time{}, Commentf("Deadline should be unset"))
}

func (s *baseAsciiClientSuite) TestGetErrorMidStream(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("VALUE key 333 100 12345\r\nunexpected eof ...")

	responses := s.client.GetMulti(ctx, []string{"key2", "key"})

	c.Assert(s.rw.sendBuf.String(), Equals, "gets key2 key\r\n")
	c.Assert(s.client.IsValidState(), IsFalse)

	c.Assert(len(responses), Equals, 2)

	resp, ok := responses["key"]
	c.Assert(ok, IsTrue)
	c.Assert(resp.Error(), NotNil)

	resp, ok = responses["key2"]
	c.Assert(ok, IsTrue)
	c.Assert(resp.Error(), NotNil)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestGetCheckEmptyBuffers(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("VALUE key 1 4 2\r\nitem\r\n")
	s.rw.recvBuf.WriteString("END\r\nextra stuff")

	resp := s.client.Get(ctx, "key")

	c.Assert(s.client.IsValidState(), IsFalse)

	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Status(), Equals, StatusNoError)
	c.Assert(resp.Key(), Equals, "key")
	c.Assert(resp.Value(), DeepEquals, []byte("item"))
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestSet(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("STORED\r\nSTORED\r\nSTORED\r\n")

	// item1 uses set because cas id is 0
	item1 := &Item{
		Key:           "key1",
		Value:         []byte("item1"),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	// item2 uses cas because cas id is 92
	item2 := &Item{
		Key:           "key2",
		Value:         []byte("i t e m 2 "),
		Flags:         234,
		DataVersionId: 92,
		Expiration:    747,
	}

	// item3 uses set because cas id is 0
	item3 := &Item{
		Key:           "key3",
		Value:         []byte("it\r\nem3"),
		Flags:         9,
		DataVersionId: 0,
		Expiration:    4,
	}

	responses := s.client.SetMulti(ctx, []*Item{item1, item2, item3})

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"set key1 123 555 5\r\nitem1\r\n"+
			"cas key2 234 747 10 92\r\ni t e m 2 \r\n"+
			"set key3 9 4 7\r\nit\r\nem3\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(len(responses), Equals, 3)
	for _, resp := range responses {
		c.Assert(resp.Error(), IsNil)
		c.Assert(resp.Status(), Equals, StatusNoError)
	}
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestSetNilItem(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	resp := s.client.Set(ctx, nil)

	c.Assert(resp.Error(), NotNil)
	c.Assert(s.rw.deadline, Equals, time.Time{}, Commentf("Deadline should be unset"))
}

func (s *baseAsciiClientSuite) TestSetBadKey(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	item := &Item{
		Key:           "b a d",
		Value:         []byte("item1"),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	resp := s.client.Set(ctx, item)

	c.Assert(s.rw.sendBuf.String(), Equals, "")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
	c.Assert(s.rw.deadline, Equals, time.Time{}, Commentf("Deadline should be unset"))
}

func (s *baseAsciiClientSuite) TestSetBadValue(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	item := &Item{
		Key:           "key",
		Value:         make([]byte, defaultMaxValueLength+1),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	resp := s.client.Set(ctx, item)

	c.Assert(s.rw.sendBuf.String(), Equals, "")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
	c.Assert(s.rw.deadline, Equals, time.Time{}, Commentf("Deadline should be unset"))
}

func (s *baseAsciiClientSuite) TestStoreNotFound(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("NOT_FOUND\r\n")

	item := &Item{
		Key:           "key",
		Value:         []byte("item"),
		Flags:         123,
		DataVersionId: 666,
		Expiration:    555,
	}

	resp := s.client.Set(ctx, item)
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"cas key 123 555 4 666\r\nitem\r\n")

	c.Assert(resp.Error(), NotNil)
	c.Assert(resp.Status(), Equals, StatusKeyNotFound)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestStoreItemNotStore(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("NOT_STORED\r\n")

	item := &Item{
		Key:           "key",
		Value:         []byte("item"),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	resp := s.client.Add(ctx, item)
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"add key 123 555 4\r\nitem\r\n")

	c.Assert(resp.Error(), NotNil)
	c.Assert(resp.Status(), Equals, StatusItemNotStored)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestStoreKeyExists(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("EXISTS\r\n")

	item := &Item{
		Key:           "key",
		Value:         []byte("item"),
		Flags:         123,
		DataVersionId: 666,
		Expiration:    555,
	}

	resp := s.client.Set(ctx, item)
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"cas key 123 555 4 666\r\nitem\r\n")

	c.Assert(resp.Error(), NotNil)
	c.Assert(resp.Status(), Equals, StatusKeyExists)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestStoreError(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("SERVER_ERROR\r\n")

	item := &Item{
		Key:           "key",
		Value:         []byte("item"),
		Flags:         123,
		DataVersionId: 666,
		Expiration:    555,
	}

	resp := s.client.Set(ctx, item)
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"cas key 123 555 4 666\r\nitem\r\n")

	c.Assert(resp.Error(), NotNil)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestStoreErrorMidStream(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("STORED\r\nSTO") // unexpected eof

	item1 := &Item{
		Key:           "key1",
		Value:         []byte("item1"),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	item2 := &Item{
		Key:           "key2",
		Value:         []byte("i t e m 2 "),
		Flags:         234,
		DataVersionId: 92,
		Expiration:    747,
	}

	item3 := &Item{
		Key:           "key3",
		Value:         []byte("it\r\nem3"),
		Flags:         9,
		DataVersionId: 0,
		Expiration:    4,
	}

	responses := s.client.SetMulti(ctx, []*Item{item1, item2, item3})

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"set key1 123 555 5\r\nitem1\r\n"+
			"cas key2 234 747 10 92\r\ni t e m 2 \r\n"+
			"set key3 9 4 7\r\nit\r\nem3\r\n")
	c.Assert(s.client.IsValidState(), IsFalse)

	c.Assert(len(responses), Equals, 3)
	c.Assert(responses[0].Error(), IsNil)
	c.Assert(responses[1].Error(), NotNil)
	c.Assert(responses[2].Error(), NotNil)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestStoreCheckEmptyBuffers(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("STORED\r\ncrap")

	item := &Item{
		Key:           "key1",
		Value:         []byte("item1"),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	resp := s.client.Set(ctx, item)

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"set key1 123 555 5\r\nitem1\r\n")
	c.Assert(s.client.IsValidState(), IsFalse)

	c.Assert(resp.Error(), IsNil)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestAdd(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("STORED\r\n")

	item := &Item{
		Key:           "key1",
		Value:         []byte("item1"),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	resp := s.client.Add(ctx, item)

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"add key1 123 555 5\r\nitem1\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestAddInvalidCasId(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	item := &Item{
		Key:           "key1",
		Value:         []byte("item1"),
		Flags:         123,
		DataVersionId: 14,
		Expiration:    555,
	}

	resp := s.client.Add(ctx, item)

	c.Assert(s.rw.sendBuf.String(), Equals, "")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
	c.Assert(s.rw.deadline, Equals, time.Time{}, Commentf("Deadline should be unset"))
}

func (s *baseAsciiClientSuite) TestReplace(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("STORED\r\n")

	item := &Item{
		Key:           "key1",
		Value:         []byte("item1"),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	resp := s.client.Replace(ctx, item)

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"replace key1 123 555 5\r\nitem1\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestAppend(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("STORED\r\n")

	resp := s.client.Append(ctx, "key", []byte("suffix"))

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"append key 0 0 6\r\nsuffix\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestPrepend(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("STORED\r\n")

	resp := s.client.Prepend(ctx, "key", []byte("prefix"))

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"prepend key 0 0 6\r\nprefix\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestDelete(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("DELETED\r\nDELETED\r\n")

	responses := s.client.DeleteMulti(ctx, []string{"key1", "key2"})

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"delete key1\r\ndelete key2\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(len(responses), Equals, 2)
	for _, resp := range responses {
		c.Assert(resp.Error(), IsNil)
		c.Assert(resp.Status(), Equals, StatusNoError)
	}
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestDeleteNotFound(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("NOT_FOUND\r\n")

	resp := s.client.Delete(ctx, "key")

	c.Assert(s.rw.sendBuf.String(), Equals, "delete key\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
	c.Assert(resp.Status(), Equals, StatusKeyNotFound)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestDeleteError(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("SERVER_ERROR\r\n")

	resp := s.client.Delete(ctx, "key")

	c.Assert(s.rw.sendBuf.String(), Equals, "delete key\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestDeleteBadKey(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	resp := s.client.Delete(ctx, "b a d")

	c.Assert(s.rw.sendBuf.String(), Equals, "")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestDeleteCheckEmptyBuffers(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("DELETED\r\nextra")

	resp := s.client.Delete(ctx, "key")

	c.Assert(s.rw.sendBuf.String(), Equals, "delete key\r\n")
	c.Assert(s.client.IsValidState(), IsFalse)

	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Status(), Equals, StatusNoError)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestIncrement(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("16\r\n")

	resp := s.client.Increment(ctx, "key", 2, 0, 0xffffffff)

	c.Assert(s.rw.sendBuf.String(), Equals, "incr key 2\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Status(), Equals, StatusNoError)
	c.Assert(resp.Key(), Equals, "key")
	c.Assert(resp.Count(), Equals, uint64(16))
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestIncrementBadKey(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	resp := s.client.Increment(ctx, "b a d", 2, 0, 0xffffffff)

	c.Assert(s.rw.sendBuf.String(), Equals, "")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
	c.Assert(s.rw.deadline, Equals, time.Time{}, Commentf("Deadline should be unset"))
}

func (s *baseAsciiClientSuite) TestIncrementBadExpiration(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	resp := s.client.Increment(ctx, "key", 2, 0, 0)

	c.Assert(s.rw.sendBuf.String(), Equals, "")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
	c.Assert(s.rw.deadline, Equals, time.Time{}, Commentf("Deadline should be unset"))
}

func (s *baseAsciiClientSuite) TestIncrementNotFound(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("NOT_FOUND\r\n")

	resp := s.client.Increment(ctx, "key", 2, 0, 0xffffffff)

	c.Assert(s.rw.sendBuf.String(), Equals, "incr key 2\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
	c.Assert(resp.Status(), Equals, StatusKeyNotFound)
	c.Assert(resp.Key(), Equals, "key")
	c.Assert(resp.Count(), Equals, uint64(0))
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestIncrementError(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("SERVER_ERROR\r\n")

	resp := s.client.Increment(ctx, "key", 2, 0, 0xffffffff)

	c.Assert(s.rw.sendBuf.String(), Equals, "incr key 2\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestIncrementCheckEmptyBuffers(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("89\r\nextra")

	resp := s.client.Increment(ctx, "key", 24, 0, 0xffffffff)

	c.Assert(s.rw.sendBuf.String(), Equals, "incr key 24\r\n")
	c.Assert(s.client.IsValidState(), IsFalse)

	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Count(), Equals, uint64(89))
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestDecrement(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("123\r\n")

	resp := s.client.Decrement(ctx, "key1", 5, 0, 0xffffffff)

	c.Assert(s.rw.sendBuf.String(), Equals, "decr key1 5\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Status(), Equals, StatusNoError)
	c.Assert(resp.Key(), Equals, "key1")
	c.Assert(resp.Count(), Equals, uint64(123))
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestFlush(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("OK\r\n")

	resp := s.client.Flush(ctx, 123)

	c.Assert(s.rw.sendBuf.String(), Equals, "flush_all 123\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestFlushError(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("SERVER_ERROR\r\n")

	resp := s.client.Flush(ctx, 0)

	c.Assert(s.rw.sendBuf.String(), Equals, "flush_all 0\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}

func (s *baseAsciiClientSuite) TestFlushCheckEmptyBuffers(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	s.rw.recvBuf.WriteString("OK\r\nextra")

	resp := s.client.Flush(ctx, 123)

	c.Assert(s.rw.sendBuf.String(), Equals, "flush_all 123\r\n")
	c.Assert(s.client.IsValidState(), IsFalse)

	c.Assert(resp.Error(), IsNil)
	if err := s.verifyContextAwareDeadline(ctx); err != nil {
		c.Error(err)
	}
}
