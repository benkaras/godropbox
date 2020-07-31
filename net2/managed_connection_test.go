package net2

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/dropbox/godropbox/resource_pool"
)

type fakeConn struct {
	net.Conn
	writeDeadline, readDeadline time.Time
}

func (f *fakeConn) SetReadDeadline(t time.Time) error {
	f.readDeadline = t
	return nil
}
func (f *fakeConn) SetWriteDeadline(t time.Time) error {
	f.readDeadline = t
	return nil
}
func (f *fakeConn) Read(b []byte) (n int, err error)  { return 0, nil }
func (f *fakeConn) Write(b []byte) (n int, err error) { return 0, nil }

type fakeManagedHandle struct {
	resource_pool.ManagedHandle
	conn net.Conn
}

func (f *fakeManagedHandle) Handle() (interface{}, error) {
	return f.conn, nil
}

func TestContextDeadline(t *testing.T) {
	options := ConnectionOptions{
		ReadTimeout:  time.Hour,
		WriteTimeout: time.Hour,
	}
	b := make([]byte, 0, 10)
	conn := &fakeConn{}
	mconn := NewManagedConn("network", "address", &fakeManagedHandle{conn: conn},
		nil, options)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	mconn.SetDeadlineFromContext(ctx)
	mconn.Read(b)
	mconn.Write(b)

	if got, want := time.Until(conn.readDeadline), time.Minute; got > want {
		t.Errorf("ContextRead() used read timeout=%s, want <= %s", got, want)
	}
	if got, want := time.Until(conn.writeDeadline), time.Minute; got > want {
		t.Errorf("ContextRead() used write timeout=%s, want <= %s", got, want)
	}
}

func TestPoolDeadline(t *testing.T) {
	options := ConnectionOptions{
		ReadTimeout:  time.Hour,
		WriteTimeout: time.Hour,
	}
	b := make([]byte, 0, 10)
	conn := &fakeConn{}
	mconn := NewManagedConn("network", "address", &fakeManagedHandle{conn: conn},
		nil, options)

	mconn.SetDeadlineFromContext(context.Background())
	mconn.Read(b)
	mconn.Write(b)

	if got, want := time.Until(conn.readDeadline), time.Hour; got > want {
		t.Errorf("ContextRead() used read timeout=%s, want <= %s", got, want)
	}
	if got, want := time.Until(conn.writeDeadline), time.Hour; got > want {
		t.Errorf("ContextRead() used write timeout=%s, want <= %s", got, want)
	}
}
