package memcache

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sync"

	"github.com/dropbox/godropbox/errors"
)

const (
	headerLength          = 24
	maxKeyLength          = 250
	defaultMaxValueLength = 1024 * 1024
	largeMaxValueLength   = 5 * 1024 * 1024 // np-large clusters support up to 5MB values.
)

func isValidKeyChar(char byte) bool {
	return (0x21 <= char && char <= 0x7e) || (0x80 <= char && char <= 0xff)
}

func isValidKeyString(key string) bool {
	if len(key) > maxKeyLength {
		return false
	}

	for _, char := range []byte(key) {
		if !isValidKeyChar(char) {
			return false
		}
	}

	return true
}

func validateValue(value []byte, maxValueLength int) error {
	if value == nil {
		return errors.New("Invalid value: cannot be nil")
	}

	if len(value) > maxValueLength {
		return errors.Newf(
			"Invalid value: length %d longer than max length %d",
			len(value),
			maxValueLength)
	}

	return nil
}

type header struct {
	Magic             uint8
	OpCode            uint8
	KeyLength         uint16
	ExtrasLength      uint8
	DataType          uint8
	VBucketIdOrStatus uint16 // vbucket id for request, status for response
	TotalBodyLength   uint32
	Opaque            uint32 // unless value
	DataVersionId     uint64 // aka CAS
}

func (h header) Serialize(buffer []byte) {
	buffer[0] = byte(h.Magic)
	buffer[1] = byte(h.OpCode)
	binary.BigEndian.PutUint16(buffer[2:], h.KeyLength)
	buffer[4] = byte(h.ExtrasLength)
	buffer[5] = byte(h.DataType)
	binary.BigEndian.PutUint16(buffer[6:], h.VBucketIdOrStatus)
	binary.BigEndian.PutUint32(buffer[8:], h.TotalBodyLength)
	binary.BigEndian.PutUint32(buffer[12:], h.Opaque)
	binary.BigEndian.PutUint64(buffer[16:], h.DataVersionId)
}

func (h *header) Deserialize(buffer []byte) {
	h.Magic = uint8(buffer[0])
	h.OpCode = uint8(buffer[1])
	h.KeyLength = binary.BigEndian.Uint16(buffer[2:])
	h.ExtrasLength = uint8(buffer[4])
	h.DataType = uint8(buffer[5])
	h.VBucketIdOrStatus = binary.BigEndian.Uint16(buffer[6:])
	h.TotalBodyLength = binary.BigEndian.Uint32(buffer[8:])
	h.Opaque = binary.BigEndian.Uint32(buffer[12:])
	h.DataVersionId = binary.BigEndian.Uint64(buffer[16:])
}

// ContextRawBinaryClient is an unsharded memcache client implementation which
// operates on a pre-existing io channel (The user must explicitly setup and
// close down the channel), using the binary memcached protocol.  Note that the
// client assumes nothing else is sending or receiving on the network channel.
// In general, all client operations are serialized (Use multiple channels /
// clients if parallelism is needed). This client accepts a context.Context which
// determines deadlines for calls.
type ContextRawBinaryClient struct {
	shard          int
	channel        DeadlineReadWriter
	mutex          sync.Mutex
	validState     bool
	maxValueLength int
}

// NewContextRawBinaryClient creates a new ContextRawBinaryClient.
func NewContextRawBinaryClient(shard int, channel DeadlineReadWriter) ContextClientShard {
	return &ContextRawBinaryClient{
		shard:          shard,
		channel:        channel,
		validState:     true,
		maxValueLength: defaultMaxValueLength,
	}
}

// NewContextLargeRawBinaryClient creates a new ContextRawBinaryClient for use with np-large cluster.
func NewContextLargeRawBinaryClient(shard int, channel DeadlineReadWriter) ContextClientShard {
	return &ContextRawBinaryClient{
		shard:          shard,
		channel:        channel,
		validState:     true,
		maxValueLength: largeMaxValueLength,
	}
}

// NewRawBinaryClient creates a new memcache unsharded memcache client
// implementation which operates on a pre-existing io channel (The user must
// explicitly setup and close down the channel), using the binary memcached
// protocol. Note that the client assumes nothing else is sending or receiving
// on the network channel.  In general, all client operations are serialized
// (Use multiple channels / clients if parallelism is needed).
func NewRawBinaryClient(shard int, channel io.ReadWriter) ClientShard {
	return newContextlessClientShardAdapter(
		NewContextRawBinaryClient(shard, &noDeadlineReadWriter{channel}),
	)
}

// NewLargeRawBinaryClient creates a new memcache binary for use with np-large cluster.
// The implementation operates on a pre-existing io channel (The user must
// explicitly setup and close down the channel), using the binary memcached
// protocol. Note that the client assumes nothing else is sending or receiving
// on the network channel.  In general, all client operations are serialized
// (Use multiple channels / clients if parallelism is needed).
func NewLargeRawBinaryClient(shard int, channel io.ReadWriter) ClientShard {
	return newContextlessClientShardAdapter(
		NewContextLargeRawBinaryClient(shard, &noDeadlineReadWriter{channel}),
	)
}

// ShardId implements the ContextClientShard interface.
// NB: The name doesn't use initialisms to be consistent with the ClientShard interface.
func (c *ContextRawBinaryClient) ShardId() int {
	return c.shard
}

// IsValidState implements the ContextClientShard interface.
func (c *ContextRawBinaryClient) IsValidState() bool {
	return c.validState
}

// Sends a memcache request through the connection.  NOTE: extras must be
// fix-sized values.
func (c *ContextRawBinaryClient) sendRequest(
	code opCode,
	dataVersionId uint64, // aka CAS
	key []byte, // may be nil
	value []byte, // may be nil
	extras ...interface{}) (err error) {

	if key != nil && len(key) > 255 {
		return errors.New("Key too long")
	}
	if !c.validState {
		// An error has occurred previously.  It's not safe to continue sending.
		return errors.New("Skipping due to previous error")
	}
	defer func() {
		if err != nil {
			c.validState = false
		}
	}()

	extrasBuffer := new(bytes.Buffer)
	for _, extra := range extras {
		err := binary.Write(extrasBuffer, binary.BigEndian, extra)
		if err != nil {
			return errors.Wrap(err, "Failed to write extra")
		}
	}

	// NOTE:
	// - memcache only supports a single dataType (0x0)
	// - vbucket id is not used by the library since vbucket related op
	//   codes are unsupported
	hdr := header{
		Magic:           reqMagicByte,
		OpCode:          byte(code),
		KeyLength:       uint16(len(key)),
		ExtrasLength:    uint8(extrasBuffer.Len()),
		TotalBodyLength: uint32(len(key) + len(value) + extrasBuffer.Len()),
		DataVersionId:   dataVersionId,
	}

	var msgBuffer = make([]byte, headerLength+hdr.TotalBodyLength)
	hdr.Serialize(msgBuffer)
	var offset uint32 = headerLength
	offset += uint32(copy(msgBuffer[offset:], extrasBuffer.Bytes()))
	if key != nil {
		offset += uint32(copy(msgBuffer[offset:], key))
	}

	if value != nil {
		offset += uint32(copy(msgBuffer[offset:], value))
	}
	if offset != headerLength+hdr.TotalBodyLength {
		return errors.New("Failed to serialize message")
	}

	bytesWritten, err := c.channel.Write(msgBuffer)
	if err != nil {
		return errors.Wrap(err, "Failed to send msg")
	}
	if bytesWritten != int((hdr.TotalBodyLength)+headerLength) {
		return errors.New("Failed to sent out message")
	}

	return nil
}

// Receive a memcache response from the connection.  The status,
// dataVersionId (aka CAS), key and value are returned, while the extra
// values are stored in the arguments.  NOTE: extras must be pointers to
// fix-sized values.
func (c *ContextRawBinaryClient) receiveResponse(
	expectedCode opCode,
	extras ...interface{}) (
	status ResponseStatus,
	dataVersionId uint64,
	key []byte, // is nil when key length is zero
	value []byte, // is nil when the value length is zero
	err error) {

	if !c.validState {
		// An error has occurred previously.  It's not safe to continue sending.
		err = errors.New("Skipping due to previous error")
		return
	}
	defer func() {
		if err != nil {
			c.validState = false
		}
	}()

	// Process the header fields
	hdr := header{}
	var hdrBytes = make([]byte, headerLength)
	hdrBytesRead, err := io.ReadFull(c.channel, hdrBytes)
	if err != nil {
		err = errors.Wrap(err, "Failed to read header")
		return
	}
	if hdrBytesRead != headerLength {
		err = errors.Newf("Failed to read header: got %d bytes, expected %d",
			hdrBytesRead, headerLength)
		return
	}
	hdr.Deserialize(hdrBytes)
	if hdr.Magic != respMagicByte {
		err = errors.Newf("Invalid response magic byte: %d", hdr.Magic)
		return
	}
	if hdr.OpCode != byte(expectedCode) {
		err = errors.Newf("Invalid response op code: %d", hdr.OpCode)
		return
	}
	if hdr.DataType != 0 {
		err = errors.Newf("Invalid data type: %d", hdr.DataType)
		return
	}

	valueLength := int(hdr.TotalBodyLength)
	valueLength -= (int(hdr.KeyLength) + int(hdr.ExtrasLength))
	if valueLength < 0 {
		err = errors.Newf("Invalid response header.  Wrong payload size.")
		return
	}

	status = ResponseStatus(hdr.VBucketIdOrStatus)
	dataVersionId = hdr.DataVersionId

	if hdr.ExtrasLength == 0 {
		if status == StatusNoError && len(extras) != 0 {
			err = errors.Newf("Expecting extras payload")
			return
		}
		// the response has no extras
	} else {
		extrasBytes := make([]byte, hdr.ExtrasLength, hdr.ExtrasLength)
		if _, err = io.ReadFull(c.channel, extrasBytes); err != nil {
			err = errors.Wrap(err, "Failed to read extra")
			return
		}

		extrasBuffer := bytes.NewBuffer(extrasBytes)

		for _, extra := range extras {
			err = binary.Read(extrasBuffer, binary.BigEndian, extra)
			if err != nil {
				err = errors.Wrap(err, "Failed to deserialize extra")
				return
			}
		}

		if extrasBuffer.Len() != 0 {
			err = errors.Newf("Not all bytes are consumed by extras fields")
			return
		}
	}

	if hdr.KeyLength > 0 {
		key = make([]byte, hdr.KeyLength, hdr.KeyLength)
		if _, err = io.ReadFull(c.channel, key); err != nil {
			err = errors.Wrap(err, "Failed to read key")
			return
		}
	}

	if valueLength > 0 {
		value = make([]byte, valueLength, valueLength)
		if _, err = io.ReadFull(c.channel, value); err != nil {
			err = errors.Wrap(err, "Failed to read value")
			return
		}
	}

	return
}

func (c *ContextRawBinaryClient) sendGetRequest(key string) GetResponse {
	if !isValidKeyString(key) {
		return NewGetErrorResponse(
			key,
			errors.New("Invalid key"))
	}

	err := c.sendRequest(opGet, 0, []byte(key), nil)
	if err != nil {
		return NewGetErrorResponse(key, err)
	}

	return nil
}

func (c *ContextRawBinaryClient) receiveGetResponse(key string) GetResponse {
	var flags uint32
	status, version, _, value, err := c.receiveResponse(opGet, &flags)
	if err != nil {
		return NewGetErrorResponse(key, err)
	}
	return NewGetResponse(key, status, flags, value, version)
}

// Get implements the ContextClient interface.
func (c *ContextRawBinaryClient) Get(ctx context.Context, key string) GetResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.channel.SetDeadlineFromContext(ctx)

	if resp := c.sendGetRequest(key); resp != nil {
		return resp
	}

	return c.receiveGetResponse(key)
}

func (c *ContextRawBinaryClient) removeDuplicateKey(keys []string) []string {
	keyMap := make(map[string]interface{})
	for _, key := range keys {
		keyMap[key] = nil
	}
	cacheKeys := make([]string, len(keyMap))
	i := 0
	for key, _ := range keyMap {
		cacheKeys[i] = key
		i = i + 1
	}
	return cacheKeys
}

// GetMulti implements the ContextClient interface.
func (c *ContextRawBinaryClient) GetMulti(ctx context.Context, keys []string) map[string]GetResponse {
	if keys == nil {
		return nil
	}

	responses := make(map[string]GetResponse)
	cacheKeys := c.removeDuplicateKey(keys)

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.channel.SetDeadlineFromContext(ctx)

	for _, key := range cacheKeys {
		if resp := c.sendGetRequest(key); resp != nil {
			responses[key] = resp
		}
	}

	for _, key := range cacheKeys {
		if _, inMap := responses[key]; inMap { // error occurred while sending
			continue
		}
		responses[key] = c.receiveGetResponse(key)
	}

	return responses
}

// GetSentinels implements the ContextClient interface.
func (c *ContextRawBinaryClient) GetSentinels(ctx context.Context, keys []string) map[string]GetResponse {
	// For raw clients, there are no difference between GetMulti and
	// GetSentinels.
	return c.GetMulti(ctx, keys)
}

func (c *ContextRawBinaryClient) sendMutateRequest(
	code opCode,
	item *Item,
	addExtras bool) MutateResponse {

	if item == nil {
		return NewMutateErrorResponse("", errors.New("item is nil"))
	}

	if !isValidKeyString(item.Key) {
		return NewMutateErrorResponse(
			item.Key,
			errors.New("Invalid key"))
	}

	if err := validateValue(item.Value, c.maxValueLength); err != nil {
		return NewMutateErrorResponse(item.Key, err)
	}

	extras := make([]interface{}, 0, 2)
	if addExtras {
		extras = append(extras, item.Flags)
		extras = append(extras, item.Expiration)
	}

	err := c.sendRequest(
		code,
		item.DataVersionId,
		[]byte(item.Key),
		item.Value,
		extras...)
	if err != nil {
		return NewMutateErrorResponse(item.Key, err)
	}
	return nil
}

func (c *ContextRawBinaryClient) receiveMutateResponse(
	code opCode,
	key string) MutateResponse {

	status, version, _, _, err := c.receiveResponse(code)
	if err != nil {
		return NewMutateErrorResponse(key, err)
	}
	return NewMutateResponse(key, status, version)
}

// Perform a mutation operation specified by the given code.
func (c *ContextRawBinaryClient) mutate(ctx context.Context, code opCode, item *Item) MutateResponse {
	if item == nil {
		return NewMutateErrorResponse("", errors.New("item is nil"))
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.channel.SetDeadlineFromContext(ctx)

	if resp := c.sendMutateRequest(code, item, true); resp != nil {
		return resp
	}

	return c.receiveMutateResponse(code, item.Key)
}

// Batch version of the mutate method.  Note that the response entries
// ordering is undefined (i.e., may not match the input ordering)
// When DataVersionId is 0, zeroVersionIdCode is used instead of code.
func (c *ContextRawBinaryClient) mutateMulti(
	ctx context.Context,
	code opCode, zeroVersionIdCode opCode,
	items []*Item) []MutateResponse {

	if items == nil {
		return nil
	}

	responses := make([]MutateResponse, len(items), len(items))

	// Short-circuit function to avoid locking.
	if len(items) == 0 {
		return responses
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.channel.SetDeadlineFromContext(ctx)

	var itemCode opCode
	for i, item := range items {
		itemCode = code
		if item.DataVersionId == 0 {
			itemCode = zeroVersionIdCode
		}
		responses[i] = c.sendMutateRequest(itemCode, item, true)
	}

	for i, item := range items {
		if responses[i] != nil { // error occurred while sending
			continue
		}
		itemCode = code
		if item.DataVersionId == 0 {
			itemCode = zeroVersionIdCode
		}
		responses[i] = c.receiveMutateResponse(itemCode, item.Key)
	}

	return responses
}

// Set implements the ContextClient interface.
func (c *ContextRawBinaryClient) Set(ctx context.Context, item *Item) MutateResponse {
	return c.mutate(ctx, opSet, item)
}

// SetMulti implements the ContextClient interface.
func (c *ContextRawBinaryClient) SetMulti(ctx context.Context, items []*Item) []MutateResponse {
	return c.mutateMulti(ctx, opSet, opSet, items)
}

// SetSentinels implements the ContextClient interface.
func (c *ContextRawBinaryClient) SetSentinels(ctx context.Context, items []*Item) []MutateResponse {
	// For raw clients, there are no difference between SetMulti and
	// SetSentinels.
	return c.SetMulti(ctx, items)
}

// CasMulti implements the ContextClient interface.
func (c *ContextRawBinaryClient) CasMulti(ctx context.Context, items []*Item) []MutateResponse {
	return c.mutateMulti(ctx, opSet, opAdd, items)
}

// CasSentinels implements the ContextClient interface.
func (c *ContextRawBinaryClient) CasSentinels(ctx context.Context, items []*Item) []MutateResponse {
	// For raw clients, there are no difference between CasMulti and
	// CasSentinels.
	return c.CasMulti(ctx, items)
}

// Add implements the ContextClient interface.
func (c *ContextRawBinaryClient) Add(ctx context.Context, item *Item) MutateResponse {
	return c.mutate(ctx, opAdd, item)
}

// AddMulti implements the ContextClient interface.
func (c *ContextRawBinaryClient) AddMulti(ctx context.Context, items []*Item) []MutateResponse {
	return c.mutateMulti(ctx, opAdd, opAdd, items)
}

// Replace implements the ContextClient interface.
func (c *ContextRawBinaryClient) Replace(ctx context.Context, item *Item) MutateResponse {
	if item == nil {
		return NewMutateErrorResponse("", errors.New("item is nil"))
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.channel.SetDeadlineFromContext(ctx)

	if resp := c.sendMutateRequest(opReplace, item, true); resp != nil {
		return resp
	}

	return c.receiveMutateResponse(opReplace, item.Key)
}

func (c *ContextRawBinaryClient) sendDeleteRequest(key string) MutateResponse {
	if !isValidKeyString(key) {
		return NewMutateErrorResponse(
			key,
			errors.New("Invalid key"))
	}

	if err := c.sendRequest(opDelete, 0, []byte(key), nil); err != nil {
		return NewMutateErrorResponse(key, err)
	}
	return nil
}

// Delete implements the ContextClient interface.
func (c *ContextRawBinaryClient) Delete(ctx context.Context, key string) MutateResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.channel.SetDeadlineFromContext(ctx)

	if resp := c.sendDeleteRequest(key); resp != nil {
		return resp
	}

	return c.receiveMutateResponse(opDelete, key)
}

// DeleteMulti implements the ContextClient interface.
func (c *ContextRawBinaryClient) DeleteMulti(ctx context.Context, keys []string) []MutateResponse {
	if keys == nil {
		return nil
	}

	responses := make([]MutateResponse, len(keys), len(keys))

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.channel.SetDeadlineFromContext(ctx)

	for i, key := range keys {
		responses[i] = c.sendDeleteRequest(key)
	}

	for i, key := range keys {
		if responses[i] != nil { // error occurred while sending
			continue
		}
		responses[i] = c.receiveMutateResponse(opDelete, key)
	}

	return responses
}

// Append implements the ContextClient interface.
func (c *ContextRawBinaryClient) Append(ctx context.Context, key string, value []byte) MutateResponse {
	item := &Item{
		Key:   key,
		Value: value,
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.channel.SetDeadlineFromContext(ctx)

	if resp := c.sendMutateRequest(opAppend, item, false); resp != nil {
		return resp
	}

	return c.receiveMutateResponse(opAppend, item.Key)
}

// Prepend implements the ContextClient interface.
func (c *ContextRawBinaryClient) Prepend(ctx context.Context, key string, value []byte) MutateResponse {
	item := &Item{
		Key:   key,
		Value: value,
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.channel.SetDeadlineFromContext(ctx)

	if resp := c.sendMutateRequest(opPrepend, item, false); resp != nil {
		return resp
	}

	return c.receiveMutateResponse(opPrepend, item.Key)
}

func (c *ContextRawBinaryClient) sendCountRequest(
	code opCode,
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	if !isValidKeyString(key) {
		return NewCountErrorResponse(
			key,
			errors.New("Invalid key"))
	}

	err := c.sendRequest(
		code,
		0,
		[]byte(key),
		nil,
		delta,
		initValue,
		expiration)
	if err != nil {
		return NewCountErrorResponse(key, err)
	}
	return nil
}

func (c *ContextRawBinaryClient) receiveCountResponse(
	code opCode,
	key string) CountResponse {

	status, _, _, value, err := c.receiveResponse(code)
	if err != nil {
		return NewCountErrorResponse(key, err)
	}

	valueBuffer := bytes.NewBuffer(value)
	var count uint64
	if err := binary.Read(valueBuffer, binary.BigEndian, &count); err != nil {
		return NewCountErrorResponse(key, err)
	}

	return NewCountResponse(key, status, count)
}

// Increment implements the ContextClient interface.
func (c *ContextRawBinaryClient) Increment(
	ctx context.Context,
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.channel.SetDeadlineFromContext(ctx)

	resp := c.sendCountRequest(opIncrement, key, delta, initValue, expiration)
	if resp != nil {
		return resp
	}
	return c.receiveCountResponse(opIncrement, key)
}

// Decrement implements the ContextClient interface.
func (c *ContextRawBinaryClient) Decrement(
	ctx context.Context,
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.channel.SetDeadlineFromContext(ctx)

	resp := c.sendCountRequest(opDecrement, key, delta, initValue, expiration)
	if resp != nil {
		return resp
	}
	return c.receiveCountResponse(opDecrement, key)
}

// Stat implements the ContextClient interface.
func (c *ContextRawBinaryClient) Stat(ctx context.Context, statsKey string) StatResponse {
	shardEntries := make(map[int](map[string]string))
	entries := make(map[string]string)
	shardEntries[c.ShardId()] = entries

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.channel.SetDeadlineFromContext(ctx)

	if !isValidKeyString(statsKey) {
		return NewStatErrorResponse(
			errors.Newf("Invalid key: %s", statsKey),
			shardEntries)
	}

	err := c.sendRequest(opStat, 0, []byte(statsKey), nil)
	if err != nil {
		return NewStatErrorResponse(err, shardEntries)
	}

	for true {
		status, _, key, value, err := c.receiveResponse(opStat)
		if err != nil {
			return NewStatErrorResponse(err, shardEntries)
		}
		if status != StatusNoError {
			// In theory, this is a valid state, but treating this as valid
			// complicates the code even more.
			c.validState = false
			return NewStatResponse(status, shardEntries)
		}
		if key == nil && value == nil { // the last entry
			break
		}
		entries[string(key)] = string(value)
	}
	return NewStatResponse(StatusNoError, shardEntries)
}

// Version implements the ContextClient interface.
func (c *ContextRawBinaryClient) Version(ctx context.Context) VersionResponse {
	versions := make(map[int]string)

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.channel.SetDeadlineFromContext(ctx)

	err := c.sendRequest(opVersion, 0, nil, nil)
	if err != nil {
		return NewVersionErrorResponse(err, versions)
	}

	status, _, _, value, err := c.receiveResponse(opVersion)
	if err != nil {
		return NewVersionErrorResponse(err, versions)
	}

	versions[c.ShardId()] = string(value)
	return NewVersionResponse(status, versions)
}

func (c *ContextRawBinaryClient) genericOp(
	ctx context.Context,
	code opCode,
	extras ...interface{}) Response {

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.channel.SetDeadlineFromContext(ctx)

	err := c.sendRequest(code, 0, nil, nil, extras...)
	if err != nil {
		return NewErrorResponse(err)
	}

	status, _, _, _, err := c.receiveResponse(code)
	if err != nil {
		return NewErrorResponse(err)
	}
	return NewResponse(status)
}

// Flush implements the ContextClient interface.
func (c *ContextRawBinaryClient) Flush(ctx context.Context, expiration uint32) Response {
	return c.genericOp(ctx, opFlush, expiration)
}

// Verbosity implements the ContextClient interface.
func (c *ContextRawBinaryClient) Verbosity(ctx context.Context, verbosity uint32) Response {
	return c.genericOp(ctx, opVerbosity, verbosity)
}
