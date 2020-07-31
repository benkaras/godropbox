package memcache

import (
	"context"
	"io"

	"github.com/dropbox/godropbox/errors"
)

// contextlessClientAdapter is a Client that passes context.Background() to all wrapped methods.
type contextlessClientAdapter struct {
	inner ContextClient
}

func newContextlessClientAdapter(inner ContextClient) Client {
	return &contextlessClientAdapter{inner}
}

// Get implements Client.Get
func (c *contextlessClientAdapter) Get(key string) GetResponse {
	return c.inner.Get(context.Background(), key)
}

// GetMulti implements Client.GetMulti
func (c *contextlessClientAdapter) GetMulti(keys []string) map[string]GetResponse {
	return c.inner.GetMulti(context.Background(), keys)
}

// GetSentinels implements Client.GetSentinels
func (c *contextlessClientAdapter) GetSentinels(keys []string) map[string]GetResponse {
	return c.inner.GetSentinels(context.Background(), keys)
}

// Set implements Client.Set
func (c *contextlessClientAdapter) Set(item *Item) MutateResponse {
	return c.inner.Set(context.Background(), item)
}

// SetMulti implements Client.SetMulti
func (c *contextlessClientAdapter) SetMulti(items []*Item) []MutateResponse {
	return c.inner.SetMulti(context.Background(), items)
}

// SetSentinels implements Client.SetSentinels
func (c *contextlessClientAdapter) SetSentinels(items []*Item) []MutateResponse {
	return c.inner.SetSentinels(context.Background(), items)
}

// CasMulti implements Client.CasMulti
func (c *contextlessClientAdapter) CasMulti(items []*Item) []MutateResponse {
	return c.inner.CasMulti(context.Background(), items)
}

// CasSentinels implements Client.CasSentinels
func (c *contextlessClientAdapter) CasSentinels(items []*Item) []MutateResponse {
	return c.inner.CasSentinels(context.Background(), items)
}

// Add implements Client.Add
func (c *contextlessClientAdapter) Add(item *Item) MutateResponse {
	return c.inner.Add(context.Background(), item)
}

// AddMulti implements Client.AddMulti
func (c *contextlessClientAdapter) AddMulti(item []*Item) []MutateResponse {
	return c.inner.AddMulti(context.Background(), item)
}

// Replace implements Client.Replace
func (c *contextlessClientAdapter) Replace(item *Item) MutateResponse {
	return c.inner.Replace(context.Background(), item)
}

// Delete implements Client.Delete
func (c *contextlessClientAdapter) Delete(key string) MutateResponse {
	return c.inner.Delete(context.Background(), key)
}

// DeleteMulti implements Client.DeleteMulti
func (c *contextlessClientAdapter) DeleteMulti(keys []string) []MutateResponse {
	return c.inner.DeleteMulti(context.Background(), keys)
}

// Append implements Client.Append
func (c *contextlessClientAdapter) Append(key string, value []byte) MutateResponse {
	return c.inner.Append(context.Background(), key, value)
}

// Prepend implements Client.Prepend
func (c *contextlessClientAdapter) Prepend(key string, value []byte) MutateResponse {
	return c.inner.Prepend(context.Background(), key, value)
}

// Increment implements Client.Increment
func (c *contextlessClientAdapter) Increment(
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {
	return c.inner.Increment(context.Background(), key, delta, initValue, expiration)
}

// Decrement implements Client.Decrement
func (c *contextlessClientAdapter) Decrement(
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {
	return c.inner.Decrement(context.Background(), key, delta, initValue, expiration)
}

// Flush implements Client.Flush
func (c *contextlessClientAdapter) Flush(expiration uint32) Response {
	return c.inner.Flush(context.Background(), expiration)
}

// Stats implements Client.Stat
func (c *contextlessClientAdapter) Stat(statsKey string) StatResponse {
	return c.inner.Stat(context.Background(), statsKey)
}

// Version implements Client.Version
func (c *contextlessClientAdapter) Version() VersionResponse {
	return c.inner.Version(context.Background())
}

// Verbosity implements Client.Verbosity
func (c *contextlessClientAdapter) Verbosity(verbosity uint32) Response {
	return c.inner.Verbosity(context.Background(), verbosity)
}

// contextlessClientShardAdapter is a ClientShard that passes context.Background() to all wrapped methods.
type contextlessClientShardAdapter struct {
	contextlessClientAdapter
	innerShard ContextClientShard
}

func newContextlessClientShardAdapter(inner ContextClientShard) ClientShard {
	return &contextlessClientShardAdapter{contextlessClientAdapter{inner}, inner}
}

// ShardId implements ClientShard.ShardId
func (c *contextlessClientShardAdapter) ShardId() int {
	return c.innerShard.ShardId()
}

// IsValidState implements ClientShard.IsValidState
func (c *contextlessClientShardAdapter) IsValidState() bool {
	return c.innerShard.IsValidState()
}

// ignoreContextClientAdapter is a ContextClient ignores the passed context.Context
type ignoreContextClientAdapter struct {
	inner Client
}

func newIgnoreContextClientAdapter(inner Client) ContextClient {
	return &ignoreContextClientAdapter{inner}
}

// Get implements Client.Get
func (c *ignoreContextClientAdapter) Get(ctx context.Context, key string) GetResponse {
	return c.inner.Get(key)
}

// GetMulti implements ContextClient.GetMulti
func (c *ignoreContextClientAdapter) GetMulti(ctx context.Context, keys []string) map[string]GetResponse {
	return c.inner.GetMulti(keys)
}

// GetSentinels implements ContextClient.GetSentinels
func (c *ignoreContextClientAdapter) GetSentinels(ctx context.Context, keys []string) map[string]GetResponse {
	return c.inner.GetSentinels(keys)
}

// Set implements ContextClient.Set
func (c *ignoreContextClientAdapter) Set(ctx context.Context, item *Item) MutateResponse {
	return c.inner.Set(item)
}

// SetMulti implements ContextClient.SetMulti
func (c *ignoreContextClientAdapter) SetMulti(ctx context.Context, items []*Item) []MutateResponse {
	return c.inner.SetMulti(items)
}

// SetSentinels implements ContextClient.SetSentinels
func (c *ignoreContextClientAdapter) SetSentinels(ctx context.Context, items []*Item) []MutateResponse {
	return c.inner.SetSentinels(items)
}

// CasMulti implements ContextClient.CasMulti
func (c *ignoreContextClientAdapter) CasMulti(ctx context.Context, items []*Item) []MutateResponse {
	return c.inner.CasMulti(items)
}

// CasSentinels implements ContextClient.CasSentinels
func (c *ignoreContextClientAdapter) CasSentinels(ctx context.Context, items []*Item) []MutateResponse {
	return c.inner.CasSentinels(items)
}

// Add implements ContextClient.Add
func (c *ignoreContextClientAdapter) Add(ctx context.Context, item *Item) MutateResponse {
	return c.inner.Add(item)
}

// AddMulti implements ContextClient.AddMulti
func (c *ignoreContextClientAdapter) AddMulti(ctx context.Context, item []*Item) []MutateResponse {
	return c.inner.AddMulti(item)
}

// Replace implements ContextClient.Replace
func (c *ignoreContextClientAdapter) Replace(ctx context.Context, item *Item) MutateResponse {
	return c.inner.Replace(item)
}

// Delete implements ContextClient.Delete
func (c *ignoreContextClientAdapter) Delete(ctx context.Context, key string) MutateResponse {
	return c.inner.Delete(key)
}

// DeleteMulti implements ContextClient.DeleteMulti
func (c *ignoreContextClientAdapter) DeleteMulti(ctx context.Context, keys []string) []MutateResponse {
	return c.inner.DeleteMulti(keys)
}

// Append implements ContextClient.Append
func (c *ignoreContextClientAdapter) Append(ctx context.Context, key string, value []byte) MutateResponse {
	return c.inner.Append(key, value)
}

// Prepend implements ContextClient.Prepend
func (c *ignoreContextClientAdapter) Prepend(ctx context.Context, key string, value []byte) MutateResponse {
	return c.inner.Prepend(key, value)
}

// Increment implements ContextClient.Increment
func (c *ignoreContextClientAdapter) Increment(ctx context.Context,
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {
	return c.inner.Increment(key, delta, initValue, expiration)
}

// Decrement implements ContextClient.Decrement
func (c *ignoreContextClientAdapter) Decrement(ctx context.Context,
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {
	return c.inner.Decrement(key, delta, initValue, expiration)
}

// Flush implements ContextClient.Flush
func (c *ignoreContextClientAdapter) Flush(ctx context.Context, expiration uint32) Response {
	return c.inner.Flush(expiration)
}

// Stats implements ContextClient.Stat
func (c *ignoreContextClientAdapter) Stat(ctx context.Context, statsKey string) StatResponse {
	return c.inner.Stat(statsKey)
}

// Version implements ContextClient.Version
func (c *ignoreContextClientAdapter) Version(ctx context.Context) VersionResponse {
	return c.inner.Version()
}

// Verbosity implements ContextClient.Verbosity
func (c *ignoreContextClientAdapter) Verbosity(ctx context.Context, verbosity uint32) Response {
	return c.inner.Verbosity(verbosity)
}

// ignoreContextClientAdapter is a ContextClientShard ignores the passed context.Context
type ignoreContextClientShardAdapter struct {
	ignoreContextClientAdapter
	innerShard ClientShard
}

func newIgnoreContextClientShardAdapter(inner ClientShard) ContextClientShard {
	return &ignoreContextClientShardAdapter{ignoreContextClientAdapter{inner}, inner}
}

// ShardId implements the ClientShard interface.
func (c *ignoreContextClientShardAdapter) ShardId() int {
	return c.innerShard.ShardId()
}

// IsValidState implements the ClientShard interface.
func (c *ignoreContextClientShardAdapter) IsValidState() bool {
	return c.innerShard.IsValidState()
}

// noDeadlineReadWriter is a DeadlineReaderWriter that ignores the context's deadline.
type noDeadlineReadWriter struct{ io.ReadWriter }

// SetDeadlineFromContext implements DeadlineReaderWriter interface.
func (n *noDeadlineReadWriter) SetDeadlineFromContext(ctx context.Context) error {
	return errors.New("cannot override deadline on a non-context client")
}
