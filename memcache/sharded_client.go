package memcache

import (
	"context"
	"expvar"

	"github.com/dropbox/godropbox/errors"
	"github.com/dropbox/godropbox/net2"
)

// A sharded memcache client implementation where sharding management is
// handled by the provided ShardManager.
type ShardedClient struct {
	manager ShardManager
	builder ContextClientShardBuilder
}

var (
	// Counters for number of get requests that successed / errored, by address.
	getOkByAddr  = expvar.NewMap("ShardedClientGetOkByAddrCounter")
	getErrByAddr = expvar.NewMap("ShardedClientGetErrByAddrCounter")
)

// NewContextShardedClient creates a new sharded memcache client.
func NewContextShardedClient(
	manager ShardManager,
	builder ContextClientShardBuilder) ContextClient {

	return &ShardedClient{
		manager: manager,
		builder: builder,
	}
}

// NewShardedClient creates a new sharded memcache client.
func NewShardedClient(
	manager ShardManager,
	builder ClientShardBuilder) Client {

	return newContextlessClientAdapter(
		&ShardedClient{
			manager: manager,
			builder: func(shard int, channel DeadlineReadWriter) ContextClientShard {
				return newIgnoreContextClientShardAdapter(builder(shard, channel))
			},
		},
	)
}

func (c *ShardedClient) release(rawClient ContextClientShard, conn net2.ManagedConn) {
	if rawClient.IsValidState() {
		_ = conn.ReleaseConnection()
	} else {
		_ = conn.DiscardConnection()
	}
}

func (c *ShardedClient) unmappedError(key string) error {
	return errors.Newf("Key '%s' does not map to any memcache shard", key)
}

func (c *ShardedClient) connectionError(shard int, err error) error {
	if err == nil {
		return errors.Newf(
			"Connection unavailable for memcache shard %d", shard)
	}
	return errors.Wrapf(
		err,
		"Connection unavailable for memcache shard %d", shard)
}

// See Client interface for documentation.
func (c *ShardedClient) Get(ctx context.Context, key string) GetResponse {
	shard, conn, err := c.manager.GetShard(key)
	if shard == -1 {
		return NewGetErrorResponse(key, c.unmappedError(key))
	}
	if err != nil {
		return NewGetErrorResponse(key, c.connectionError(shard, err))
	}
	if conn == nil {
		// NOTE: zero is an invalid version id.
		return NewGetResponse(key, StatusKeyNotFound, 0, nil, 0)
	}

	client := c.builder(shard, conn)
	defer c.release(client, conn)

	result := client.Get(ctx, key)
	if client.IsValidState() {
		getOkByAddr.Add(conn.Key().Address, 1)
	} else {
		getErrByAddr.Add(conn.Key().Address, 1)
	}
	return result
}

func (c *ShardedClient) getMultiHelper(
	ctx context.Context,
	shard int,
	conn net2.ManagedConn,
	connErr error,
	keys []string,
	resultsChannel chan map[string]GetResponse) {

	var results map[string]GetResponse
	if shard == -1 {
		results = make(map[string]GetResponse)
		for _, key := range keys {
			results[key] = NewGetErrorResponse(key, c.unmappedError(key))
		}
	} else if connErr != nil {
		results = make(map[string]GetResponse)
		for _, key := range keys {
			results[key] = NewGetErrorResponse(
				key,
				c.connectionError(shard, connErr))
		}
	} else if conn == nil {
		results = make(map[string]GetResponse)
		for _, key := range keys {
			// NOTE: zero is an invalid version id.
			results[key] = NewGetResponse(key, StatusKeyNotFound, 0, nil, 0)
		}
	} else {
		client := c.builder(shard, conn)
		defer c.release(client, conn)

		results = client.GetMulti(ctx, keys)
		if client.IsValidState() {
			getOkByAddr.Add(conn.Key().Address, 1)
		} else {
			getErrByAddr.Add(conn.Key().Address, 1)
		}
	}
	resultsChannel <- results
}

// See Client interface for documentation.
func (c *ShardedClient) GetMulti(ctx context.Context, keys []string) map[string]GetResponse {
	return c.getMulti(ctx, c.manager.GetShardsForKeys(keys))
}

// See Client interface for documentation.
func (c *ShardedClient) GetSentinels(ctx context.Context, keys []string) map[string]GetResponse {
	return c.getMulti(ctx, c.manager.GetShardsForSentinelsFromKeys(keys))
}

func (c *ShardedClient) getMulti(ctx context.Context, shardMapping map[int]*ShardMapping) map[string]GetResponse {
	resultsChannel := make(chan map[string]GetResponse, len(shardMapping))
	for shard, mapping := range shardMapping {
		go c.getMultiHelper(
			ctx,
			shard,
			mapping.Connection,
			mapping.ConnErr,
			mapping.Keys,
			resultsChannel)
	}

	results := make(map[string]GetResponse)
	for i := 0; i < len(shardMapping); i++ {
		for key, resp := range <-resultsChannel {
			results[key] = resp
		}
	}
	return results
}

func (c *ShardedClient) mutate(
	ctx context.Context,
	key string,
	mutateFunc func(context.Context, ContextClient) MutateResponse) MutateResponse {
	shard, conn, err := c.manager.GetShard(key)
	if shard == -1 {
		return NewMutateErrorResponse(key, c.unmappedError(key))
	}
	if err != nil {
		return NewMutateErrorResponse(key, c.connectionError(shard, err))
	}
	if conn == nil {
		// NOTE: zero is an invalid version id.
		return NewMutateResponse(key, StatusNoError, 0)
	}

	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return mutateFunc(ctx, client)
}

// See Client interface for documentation.
func (c *ShardedClient) Set(ctx context.Context, item *Item) MutateResponse {
	return c.mutate(
		ctx,
		item.Key,
		func(ctx context.Context, shardClient ContextClient) MutateResponse {
			return shardClient.Set(ctx, item)
		})
}

func (c *ShardedClient) mutateMultiHelper(
	ctx context.Context,
	mutateMultiFunc func(context.Context, ContextClient, *ShardMapping) []MutateResponse,
	shard int,
	mapping *ShardMapping,
	resultsChannel chan []MutateResponse) {

	keys := mapping.Keys
	conn := mapping.Connection
	connErr := mapping.ConnErr
	warmingUp := mapping.WarmingUp

	var results []MutateResponse
	if shard == -1 {
		results = make([]MutateResponse, 0, len(keys))
		for _, key := range keys {
			results = append(
				results,
				NewMutateErrorResponse(key, c.unmappedError(key)))
		}
	} else if connErr != nil {
		results = make([]MutateResponse, 0, len(keys))
		for _, key := range keys {
			results = append(
				results,
				NewMutateErrorResponse(key, c.connectionError(shard, connErr)))
		}
	} else if conn == nil {
		results = make([]MutateResponse, 0, len(keys))
		for _, key := range keys {
			// NOTE: zero is an invalid version id.
			results = append(
				results,
				NewMutateResponse(key, StatusNoError, 0))
		}
	} else {
		client := c.builder(shard, conn)
		defer c.release(client, conn)

		results = mutateMultiFunc(ctx, client, mapping)
	}

	// If server is warming up, we override all failures with success message.
	if warmingUp {
		for idx, key := range keys {
			if results[idx].Error() != nil {
				results[idx] = NewMutateResponse(key, StatusNoError, 0)
			}
		}
	}

	resultsChannel <- results
}

// See Client interface for documentation.
func (c *ShardedClient) mutateMulti(
	ctx context.Context,
	shards map[int]*ShardMapping,
	mutateMultiFunc func(context.Context, ContextClient, *ShardMapping) []MutateResponse) []MutateResponse {

	numKeys := 0

	resultsChannel := make(chan []MutateResponse, len(shards))
	for shard, mapping := range shards {
		numKeys += len(mapping.Keys)
		go c.mutateMultiHelper(
			ctx,
			mutateMultiFunc,
			shard,
			mapping,
			resultsChannel)
	}

	results := make([]MutateResponse, 0, numKeys)
	for i := 0; i < len(shards); i++ {
		results = append(results, (<-resultsChannel)...)
	}
	return results
}

// A helper used to specify a SetMulti mutation operation on a shard client.
func setMultiMutator(ctx context.Context, shardClient ContextClient, mapping *ShardMapping) []MutateResponse {
	return shardClient.SetMulti(ctx, mapping.Items)
}

// A helper used to specify a CasMulti mutation operation on a shard client.
func casMultiMutator(ctx context.Context, shardClient ContextClient, mapping *ShardMapping) []MutateResponse {
	return shardClient.CasMulti(ctx, mapping.Items)
}

// See Client interface for documentation.
func (c *ShardedClient) SetMulti(ctx context.Context, items []*Item) []MutateResponse {
	return c.mutateMulti(ctx, c.manager.GetShardsForItems(items), setMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) SetSentinels(ctx context.Context, items []*Item) []MutateResponse {
	return c.mutateMulti(ctx, c.manager.GetShardsForSentinelsFromItems(items), setMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) CasMulti(ctx context.Context, items []*Item) []MutateResponse {
	return c.mutateMulti(ctx, c.manager.GetShardsForItems(items), casMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) CasSentinels(ctx context.Context, items []*Item) []MutateResponse {
	return c.mutateMulti(ctx, c.manager.GetShardsForSentinelsFromItems(items), casMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) Add(ctx context.Context, item *Item) MutateResponse {
	return c.mutate(
		ctx,
		item.Key,
		func(ctx context.Context, shardClient ContextClient) MutateResponse {
			return shardClient.Add(ctx, item)
		})
}

// A helper used to specify a AddMulti mutation operation on a shard client.
func addMultiMutator(ctx context.Context, shardClient ContextClient, mapping *ShardMapping) []MutateResponse {
	return shardClient.AddMulti(ctx, mapping.Items)
}

// See Client interface for documentation.
func (c *ShardedClient) AddMulti(ctx context.Context, items []*Item) []MutateResponse {
	return c.mutateMulti(ctx, c.manager.GetShardsForItems(items), addMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) Replace(ctx context.Context, item *Item) MutateResponse {
	return c.mutate(
		ctx,
		item.Key,
		func(ctx context.Context, shardClient ContextClient) MutateResponse {
			return shardClient.Replace(ctx, item)
		})
}

// See Client interface for documentation.
func (c *ShardedClient) Delete(ctx context.Context, key string) MutateResponse {
	return c.mutate(
		ctx,
		key,
		func(ctx context.Context, shardClient ContextClient) MutateResponse {
			return shardClient.Delete(ctx, key)
		})
}

// A helper used to specify a DeleteMulti mutation operation on a shard client.
func deleteMultiMutator(ctx context.Context, shardClient ContextClient, mapping *ShardMapping) []MutateResponse {
	return shardClient.DeleteMulti(ctx, mapping.Keys)
}

// See Client interface for documentation.
func (c *ShardedClient) DeleteMulti(ctx context.Context, keys []string) []MutateResponse {
	return c.mutateMulti(ctx, c.manager.GetShardsForKeys(keys), deleteMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) Append(ctx context.Context, key string, value []byte) MutateResponse {
	return c.mutate(
		ctx,
		key,
		func(ctx context.Context, shardClient ContextClient) MutateResponse {
			return shardClient.Append(ctx, key, value)
		})
}

// See Client interface for documentation.
func (c *ShardedClient) Prepend(ctx context.Context, key string, value []byte) MutateResponse {
	return c.mutate(
		ctx,
		key,
		func(ctx context.Context, shardClient ContextClient) MutateResponse {
			return shardClient.Prepend(ctx, key, value)
		})
}

func (c *ShardedClient) count(
	ctx context.Context,
	key string,
	countFunc func(context.Context, ContextClient) CountResponse) CountResponse {
	shard, conn, err := c.manager.GetShard(key)
	if shard == -1 {
		return NewCountErrorResponse(key, c.unmappedError(key))
	}
	if err != nil {
		return NewCountErrorResponse(key, c.connectionError(shard, err))
	}
	if conn == nil {
		return NewCountResponse(key, StatusNoError, 0)
	}

	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return countFunc(ctx, client)
}

// See Client interface for documentation.
func (c *ShardedClient) Increment(ctx context.Context,
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	return c.count(
		ctx,
		key,
		func(ctx context.Context, shardClient ContextClient) CountResponse {
			return shardClient.Increment(ctx, key, delta, initValue, expiration)
		})
}

// See Client interface for documentation.
func (c *ShardedClient) Decrement(ctx context.Context,
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	return c.count(
		ctx,
		key,
		func(ctx context.Context, shardClient ContextClient) CountResponse {
			return shardClient.Decrement(ctx, key, delta, initValue, expiration)
		})
}

func (c *ShardedClient) flushHelper(
	ctx context.Context,
	shard int,
	conn net2.ManagedConn,
	expiration uint32) Response {

	if conn == nil {
		return NewErrorResponse(c.connectionError(shard, nil))
	}
	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return client.Flush(ctx, expiration)
}

// See Client interface for documentation.
func (c *ShardedClient) Flush(ctx context.Context, expiration uint32) Response {
	var err error
	for shard, conn := range c.manager.GetAllShards() {
		response := c.flushHelper(ctx, shard, conn, expiration)
		if response.Error() != nil {
			if err == nil {
				err = response.Error()
			} else {
				err = errors.Wrap(response.Error(), err.Error())
			}
		}
	}

	if err != nil {
		return NewErrorResponse(err)
	}

	return NewResponse(StatusNoError)
}

func (c *ShardedClient) statHelper(
	ctx context.Context,
	shard int,
	conn net2.ManagedConn,
	statsKey string) StatResponse {

	if conn == nil {
		return NewStatErrorResponse(
			c.connectionError(shard, nil),
			make(map[int](map[string]string)))
	}
	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return client.Stat(ctx, statsKey)
}

// See Client interface for documentation.
func (c *ShardedClient) Stat(ctx context.Context, statsKey string) StatResponse {
	statEntries := make(map[int](map[string]string))

	var err error
	for shard, conn := range c.manager.GetAllShards() {
		response := c.statHelper(ctx, shard, conn, statsKey)
		if response.Error() != nil {
			if err == nil {
				err = response.Error()
			} else {
				err = errors.Wrap(response.Error(), err.Error())
			}
		}

		for shardId, entries := range response.Entries() {
			statEntries[shardId] = entries
		}
	}

	if err != nil {
		return NewStatErrorResponse(err, statEntries)
	}

	return NewStatResponse(StatusNoError, statEntries)
}

func (c *ShardedClient) versionHelper(
	ctx context.Context,
	shard int,
	conn net2.ManagedConn) VersionResponse {

	if conn == nil {
		return NewVersionErrorResponse(
			c.connectionError(shard, nil),
			make(map[int]string))
	}
	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return client.Version(ctx)
}

// See Client interface for documentation.
func (c *ShardedClient) Version(ctx context.Context) VersionResponse {
	shardConns := c.manager.GetAllShards()

	var err error
	versions := make(map[int]string)
	for shard, conn := range shardConns {
		response := c.versionHelper(ctx, shard, conn)
		if response.Error() != nil {
			if err == nil {
				err = response.Error()
			} else {
				err = errors.Wrap(response.Error(), err.Error())
			}
			continue
		}

		for shardId, versionString := range response.Versions() {
			versions[shardId] = versionString
		}
	}

	if err != nil {
		return NewVersionErrorResponse(err, versions)
	}

	return NewVersionResponse(StatusNoError, versions)
}

func (c *ShardedClient) verbosityHelper(
	ctx context.Context,
	shard int,
	conn net2.ManagedConn,
	verbosity uint32) Response {

	if conn == nil {
		return NewErrorResponse(c.connectionError(shard, nil))
	}
	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return client.Verbosity(ctx, verbosity)
}

// See Client interface for documentation.
func (c *ShardedClient) Verbosity(ctx context.Context, verbosity uint32) Response {
	var err error
	for shard, conn := range c.manager.GetAllShards() {
		response := c.verbosityHelper(ctx, shard, conn, verbosity)
		if response.Error() != nil {
			if err == nil {
				err = response.Error()
			} else {
				err = errors.Wrap(response.Error(), err.Error())
			}
		}
	}

	if err != nil {
		return NewErrorResponse(err)
	}

	return NewResponse(StatusNoError)
}
