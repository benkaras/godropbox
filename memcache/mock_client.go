package memcache

import (
	"context"
	"strconv"
	"sync"

	"github.com/dropbox/godropbox/errors"
)

type MockClient struct {
	data                   map[string]*Item
	version                uint64
	mutex                  sync.Mutex
	forceGetMisses         bool // return StatusKeyNotFound for all gets
	forceSetInternalErrors bool // return StatusInternalError for all sets
	forceFailEverything    bool // return StatusInternalError for all functions
}
type Operation int

const (
	Increment Operation = iota
	Decrement
)

// NewMockContextClient returns a fake client.
func NewMockContextClient() ContextClientShard {
	return &MockClient{data: make(map[string]*Item)}
}

// NewMockContextClientErrorAllSets returns a fake client that fails all sets.
func NewMockContextClientErrorAllSets() ContextClientShard {
	return &MockClient{data: make(map[string]*Item), forceSetInternalErrors: true}
}

// NewMockContextClientMissAllGets returns a fake client that fails all gets.
func NewMockContextClientMissAllGets() ContextClientShard {
	return &MockClient{data: make(map[string]*Item), forceGetMisses: true}
}

// NewMockContextClientFailEverything returns a fake client that fails all calls.
func NewMockContextClientFailEverything() ContextClientShard {
	return &MockClient{data: make(map[string]*Item), forceFailEverything: true}
}

// NewMockClient returns a fake client.
func NewMockClient() ClientShard {
	return newContextlessClientShardAdapter(&MockClient{data: make(map[string]*Item)})
}

// NewMockClientErrorAllSets returns a fake client that fails all sets.
func NewMockClientErrorAllSets() ClientShard {
	return newContextlessClientShardAdapter(&MockClient{data: make(map[string]*Item), forceSetInternalErrors: true})
}

// NewMockClientMissAllGets returns a fake client that fails all gets.
func NewMockClientMissAllGets() ClientShard {
	return newContextlessClientShardAdapter(&MockClient{data: make(map[string]*Item), forceGetMisses: true})
}

// NewMockClientFailEverything returns a fake client that fails all calls.
func NewMockClientFailEverything() ClientShard {
	return newContextlessClientShardAdapter(&MockClient{data: make(map[string]*Item), forceFailEverything: true})
}

// IsValidState implements the ContextClientShard interface.
func (c *MockClient) IsValidState() bool {
	return !c.forceFailEverything
}

// ShardId implements the ContextClientShard interface.
func (c *MockClient) ShardId() int {
	return 0
}

func (c *MockClient) getHelper(key string) GetResponse {
	if c.forceFailEverything {
		return NewGetResponse(
			key, StatusInternalError, 0, nil, 0)
	}
	if v, ok := c.data[key]; ok && !c.forceGetMisses {
		return NewGetResponse(
			key,
			StatusNoError,
			v.Flags,
			v.Value,
			v.DataVersionId)
	}
	return NewGetResponse(key, StatusKeyNotFound, 0, nil, 0)
}

// Get implements the ContextClient interface. This retrieves a single entry from memcache.
func (c *MockClient) Get(ctx context.Context, key string) GetResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.getHelper(key)
}

// GetMulti implements the ContextClient interface. Batch version of the Get method.
func (c *MockClient) GetMulti(ctx context.Context, keys []string) map[string]GetResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	res := make(map[string]GetResponse)
	for _, key := range keys {
		res[key] = c.getHelper(key)
	}
	return res
}

// GetSentinels implements the ContextClient interface.
func (c *MockClient) GetSentinels(ctx context.Context, keys []string) map[string]GetResponse {
	return c.GetMulti(ctx, keys)
}

func (c *MockClient) setHelper(item *Item) MutateResponse {
	c.version++
	if c.forceSetInternalErrors || c.forceFailEverything {
		return NewMutateResponse(
			item.Key,
			StatusInternalError,
			0)
	}

	newItem := &Item{
		Key:           item.Key,
		Value:         item.Value,
		Flags:         item.Flags,
		Expiration:    item.Expiration,
		DataVersionId: c.version,
	}

	existing, ok := c.data[newItem.Key]

	if item.DataVersionId == 0 ||
		(ok && item.DataVersionId == existing.DataVersionId) {

		c.data[newItem.Key] = newItem
		return NewMutateResponse(
			newItem.Key,
			StatusNoError,
			newItem.DataVersionId)
	} else if !ok {
		return NewMutateResponse(
			newItem.Key,
			StatusKeyNotFound,
			0)
	} else {
		// CAS mismatch
		return NewMutateResponse(
			newItem.Key,
			StatusKeyExists,
			0)
	}

}

func (c *MockClient) casHelper(item *Item) MutateResponse {
	if item.DataVersionId == 0 {
		return c.addHelper(item)
	} else {
		return c.setHelper(item)
	}
}

// Set implements the ContextClient interface.
// This sets a single entry into memcache.  If the item's data version id
// (aka CAS) is nonzero, the set operation can only succeed if the item
// exists in memcache and has a same data version id.
func (c *MockClient) Set(ctx context.Context, item *Item) MutateResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.setHelper(item)
}

// SetMulti implements the ContextClient interface.
// Batch version of the Set method.  Note that the response entries
// ordering is undefined (i.e., may not match the input ordering).
func (c *MockClient) SetMulti(ctx context.Context, items []*Item) []MutateResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	res := make([]MutateResponse, len(items))
	for i, item := range items {
		res[i] = c.setHelper(item)
	}
	return res
}

// SetSentinels implements the ContextClient interface.
func (c *MockClient) SetSentinels(ctx context.Context, items []*Item) []MutateResponse {
	// TODO(patrick): Support state mocking
	return c.SetMulti(ctx, items)
}

// CasMulti implements the ContextClient interface.
func (c *MockClient) CasMulti(ctx context.Context, items []*Item) []MutateResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	res := make([]MutateResponse, len(items))
	for i, item := range items {
		res[i] = c.casHelper(item)
	}
	return res
}

// CasSentinels implements the ContextClient interface.
func (c *MockClient) CasSentinels(ctx context.Context, items []*Item) []MutateResponse {
	return c.CasMulti(ctx, items)
}

func (c *MockClient) addHelper(item *Item) MutateResponse {
	c.version++
	if c.forceFailEverything {
		return NewMutateResponse(
			item.Key,
			StatusInternalError,
			0)
	}
	newItem := &Item{
		Key:           item.Key,
		Value:         item.Value,
		Flags:         item.Flags,
		Expiration:    item.Expiration,
		DataVersionId: c.version,
	}

	if _, ok := c.data[newItem.Key]; !ok {
		c.data[newItem.Key] = newItem
		return NewMutateResponse(
			newItem.Key,
			StatusNoError,
			newItem.DataVersionId)
	} else {
		return NewMutateResponse(
			newItem.Key,
			StatusItemNotStored,
			0)
	}
}

// Add implements the ContextClient interface.
// This adds a single entry into memcache.  Note: Add will fail if the
// item already exist in memcache.
func (c *MockClient) Add(ctx context.Context, item *Item) MutateResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.addHelper(item)
}

// AddMulti implements the ContextClient interface.
// Batch version of the Add method.  Note that the response entries
// ordering is undefined (i.e., may not match the input ordering).
func (c *MockClient) AddMulti(ctx context.Context, items []*Item) []MutateResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	res := make([]MutateResponse, len(items))
	for i, item := range items {
		res[i] = c.addHelper(item)
	}
	return res
}

// Replace implements the ContextClient interface.
// This replaces a single entry in memcache.  Note: Replace will fail if
// the does not exist in memcache.
func (c *MockClient) Replace(ctx context.Context, item *Item) MutateResponse {
	return NewMutateErrorResponse(
		item.Key,
		errors.Newf("Replace not implemented"))
}

// Delete implements the ContextClient interface.
// This deletes a single entry from memcache.
func (c *MockClient) Delete(ctx context.Context, key string) MutateResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.forceFailEverything {
		return NewMutateResponse(
			key,
			StatusInternalError,
			0)
	}

	_, ok := c.data[key]
	if !ok {
		return NewMutateResponse(
			key,
			StatusKeyNotFound,
			0)
	}

	delete(c.data, key)

	return NewMutateResponse(
		key,
		StatusNoError,
		0)
}

// DeleteMulti implements the ContextClient interface.
// Batch version of the Delete method.  Note that the response entries
// ordering is undefined (i.e., may not match the input ordering)
func (c *MockClient) DeleteMulti(ctx context.Context, keys []string) []MutateResponse {
	res := make([]MutateResponse, len(keys))
	for i, key := range keys {
		res[i] = c.Delete(ctx, key)
	}
	return res
}

// Append implements the ContextClient interface.
// This appends the value bytes to the end of an existing entry.  Note that
// this does not allow you to extend past the item limit.
func (c *MockClient) Append(ctx context.Context, key string, value []byte) MutateResponse {
	return NewMutateErrorResponse(key, errors.Newf("Append not implemented"))
}

// Prepend implements the ContextClient interface.
// This prepends the value bytes to the end of an existing entry.  Note that
// this does not allow you to extend past the item limit.
func (c *MockClient) Prepend(ctx context.Context, key string, value []byte) MutateResponse {
	return NewMutateErrorResponse(key, errors.Newf("Prepend not implemented"))
}

func (c *MockClient) incrementDecrementHelper(
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32, operation Operation) CountResponse {

	if c.forceFailEverything {
		return NewCountResponse(key, StatusInternalError, 0)
	}

	if v, ok := c.data[key]; ok && !c.forceGetMisses {
		// item already exists
		valStr, flags, dataVersionId, expiration := v.Value, v.Flags, v.DataVersionId, v.Expiration
		value, err := strconv.Atoi(string(valStr))
		if err != nil {
			return NewCountResponse(key, StatusIncrDecrOnNonNumericValue, 0)
		}
		var newValue uint64
		if operation == Increment {
			newValue = uint64(value) + delta
		} else {
			newValue = uint64(value) - delta
			if newValue < 0 {
				newValue = 0
			}
		}
		c.setHelper(&Item{
			Key:           key,
			Value:         []byte(strconv.Itoa(int(newValue))),
			Flags:         flags,
			Expiration:    expiration,
			DataVersionId: dataVersionId,
		})
		return NewCountResponse(key, StatusNoError, newValue)
	}
	if expiration == 0xffffffff {
		return NewCountResponse(key, StatusKeyNotFound, 0)
	} else {
		var newValue uint64
		if operation == Increment {
			newValue = initValue + delta
		} else {
			newValue = initValue - delta
			if newValue < 0 {
				newValue = 0
			}
		}
		c.addHelper(&Item{
			Key:        key,
			Value:      []byte(strconv.Itoa(int(newValue))),
			Flags:      0,
			Expiration: expiration,
		})
		return NewCountResponse(key, StatusNoError, newValue)
	}

}

// Increment implements the ContextClient interface.
// This increments the key's counter by delta.  If the counter does not
// exist, one of two things may happen:
// 1. If the expiration value is all one-bits (0xffffffff), the operation
// will fail with StatusNotFound.
// 2. For all other expiration values, the operation will succeed by
// seeding the value for this key with the provided initValue to expire
// with the provided expiration time. The flags will be set to zero.
//
// NOTE:
// 1. If you want to set the value of the counter with add/set/replace,
// the objects data must be the ascii representation of the value and
// not the byte values of a 64 bit integer.
// 2. Incrementing the counter may cause the counter to wrap.
func (c *MockClient) Increment(ctx context.Context,
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.incrementDecrementHelper(key, delta, initValue, expiration, Increment)

}

// Decrement implements the ContextClient interface.
// This decrements the key's counter by delta.  If the counter does not
// exist, one of two things may happen:
// 1. If the expiration value is all one-bits (0xffffffff), the operation
// will fail with StatusNotFound.
// 2. For all other expiration values, the operation will succeed by
// seeding the value for this key with the provided initValue to expire
// with the provided expiration time. The flags will be set to zero.
//
// NOTE:
// 1. If you want to set the value of the counter with add/set/replace,
// the objects data must be the ascii representation of the value and
// not the byte values of a 64 bit integer.
// 2. Decrementing a counter will never result in a "negative value" (or
// cause the counter to "wrap"). instead the counter is set to 0.
func (c *MockClient) Decrement(ctx context.Context,
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.incrementDecrementHelper(key, delta, initValue, expiration, Decrement)

}

// Flush implements the ContextClient interface.
// This invalidates all existing cache items after expiration number of
// seconds.
func (c *MockClient) Flush(ctx context.Context, expiration uint32) Response {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// TODO(patrick): Use expiration argument
	c.data = make(map[string]*Item)
	return NewResponse(StatusNoError)
}

// Stat implements the ContextClient interface.
// This requests the server statistics. When the key is an empty string,
// the server will respond with a "default" set of statistics information.
func (c *MockClient) Stat(ctx context.Context, statsKey string) StatResponse {
	return NewStatErrorResponse(errors.Newf("Stat not implemented"), nil)
}

// Version implements the ContextClient interface.
// This returns the server's version string.
func (c *MockClient) Version(ctx context.Context) VersionResponse {
	return NewVersionResponse(StatusNoError, map[int]string{0: "MockSever"})
}

// Verbosity implements the ContextClient interface.
// This set the verbosity level of the server.
func (c *MockClient) Verbosity(ctx context.Context, verbosity uint32) Response {
	return NewResponse(StatusNoError)
}
