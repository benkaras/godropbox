package net2

import (
	"context"
	"net"
	"time"

	"github.com/dropbox/godropbox/errors"
	"github.com/dropbox/godropbox/resource_pool"
)

// Dial's arguments.
type NetworkAddress struct {
	Network string
	Address string
}

// A connection managed by a connection pool.  NOTE: SetDeadline,
// Setdeadline and Setdeadline are disabled for managed connections.
// The deadline from the context is used if present, otherwise deadlines
// are set by the connection pool.
type ManagedConn interface {
	net.Conn

	// This returns the original (network, address) entry used for creating
	// the connection.
	Key() NetworkAddress

	// This returns the underlying net.Conn implementation.
	RawConn() net.Conn

	// This returns the connection pool which owns this connection.
	Owner() ConnectionPool

	// This indictes a user is done with the connection and releases the
	// connection back to the connection pool.
	ReleaseConnection() error

	// This indicates the connection is an invalid state, and that the
	// connection should be discarded from the connection pool.
	DiscardConnection() error

	// Sets the read/write deadline from the context.
	SetDeadlineFromContext(ctx context.Context) error
}

// A physical implementation of ManagedConn
type managedConnImpl struct {
	addr     NetworkAddress
	handle   resource_pool.ManagedHandle
	pool     ConnectionPool
	options  ConnectionOptions
	deadline time.Time // The read/write deadline. If zero, then options sets the deadline.
}

// This creates a managed connection wrapper.
func NewManagedConn(
	network string,
	address string,
	handle resource_pool.ManagedHandle,
	pool ConnectionPool,
	options ConnectionOptions) ManagedConn {

	addr := NetworkAddress{
		Network: network,
		Address: address,
	}

	return &managedConnImpl{
		addr:    addr,
		handle:  handle,
		pool:    pool,
		options: options,
	}
}

func (c *managedConnImpl) rawConn() (net.Conn, error) {
	h, err := c.handle.Handle()
	return h.(net.Conn), err
}

// See ManagedConn for documentation.
func (c *managedConnImpl) RawConn() net.Conn {
	h, _ := c.handle.Handle()
	return h.(net.Conn)
}

// See ManagedConn for documentation.
func (c *managedConnImpl) Key() NetworkAddress {
	return c.addr
}

// See ManagedConn for documentation.
func (c *managedConnImpl) Owner() ConnectionPool {
	return c.pool
}

// See ManagedConn for documentation.
func (c *managedConnImpl) ReleaseConnection() error {
	return c.handle.Release()
}

// See ManagedConn for documentation.
func (c *managedConnImpl) DiscardConnection() error {
	return c.handle.Discard()
}

// See net.Conn for documentation
func (c *managedConnImpl) Read(b []byte) (n int, err error) {
	conn, err := c.rawConn()
	if err != nil {
		return 0, err
	}

	if !c.deadline.IsZero() {
		_ = conn.SetReadDeadline(c.deadline)
	} else if c.options.ReadTimeout > 0 {
		deadline := c.options.getCurrentTime().Add(c.options.ReadTimeout)
		_ = conn.SetReadDeadline(deadline)
	}
	n, err = conn.Read(b)
	if err != nil {
		var localAddr string
		if conn.LocalAddr() != nil {
			localAddr = conn.LocalAddr().String()
		} else {
			localAddr = "(nil)"
		}

		var remoteAddr string
		if conn.RemoteAddr() != nil {
			remoteAddr = conn.RemoteAddr().String()
		} else {
			remoteAddr = "(nil)"
		}
		err = errors.Wrapf(err, "Read error from host: %s <-> %s", localAddr, remoteAddr)
	}
	return
}

// See net.Conn for documentation
func (c *managedConnImpl) Write(b []byte) (n int, err error) {
	conn, err := c.rawConn()
	if err != nil {
		return 0, err
	}

	if !c.deadline.IsZero() {
		_ = conn.SetWriteDeadline(c.deadline)
	} else if c.options.WriteTimeout > 0 {
		deadline := c.options.getCurrentTime().Add(c.options.WriteTimeout)
		_ = conn.SetWriteDeadline(deadline)
	}
	n, err = conn.Write(b)
	if err != nil {
		err = errors.Wrap(err, "Write error")
	}
	return
}

// See net.Conn for documentation
func (c *managedConnImpl) Close() error {
	return c.handle.Discard()
}

// See net.Conn for documentation
func (c *managedConnImpl) LocalAddr() net.Addr {
	conn, _ := c.rawConn()
	return conn.LocalAddr()
}

// See net.Conn for documentation
func (c *managedConnImpl) RemoteAddr() net.Addr {
	conn, _ := c.rawConn()
	return conn.RemoteAddr()
}

// SetDeadline is disabled for managed connection (The deadline is set by
// us, with respect to the read/write timeouts specified in ConnectionOptions).
func (c *managedConnImpl) SetDeadline(t time.Time) error {
	return errors.New("Cannot set deadline for managed connection")
}

// SetReadDeadline is disabled for managed connection (The deadline is set by
// us with respect to the read timeout specified in ConnectionOptions).
func (c *managedConnImpl) SetReadDeadline(t time.Time) error {
	return errors.New("Cannot set read deadline for managed connection")
}

// SetWriteDeadline is disabled for managed connection (The deadline is set by
// us with respect to the write timeout specified in ConnectionOptions).
func (c *managedConnImpl) SetWriteDeadline(t time.Time) error {
	return errors.New("Cannot set write deadline for managed connection")
}

// SetDeadlineFromContext implements the DeadlineReadWriter interface.
func (c *managedConnImpl) SetDeadlineFromContext(ctx context.Context) error {
	if d, ok := ctx.Deadline(); ok {
		c.deadline = d
	} else {
		c.deadline = time.Time{}
	}
	return nil
}
