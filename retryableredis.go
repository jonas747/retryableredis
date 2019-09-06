package retryableredis

import (
	"bufio"
	"io"
	"net"
	"strings"
	"time"

	"github.com/mediocregopher/radix/v3"
	"github.com/mediocregopher/radix/v3/resp"
)

type retryableRedisConn struct {
	inner radix.Conn

	conf *DialConfig
}

type DialConfig struct {
	Network, Addr string
	OnReconnect   func(error)
	OnRetry       func(error)
	DialOpts      []radix.DialOpt
}

func Dial(conf *DialConfig) (radix.Conn, error) {
	rc := &retryableRedisConn{
		conf: conf,
	}

	err := rc.Reconnect(nil)
	return rc, err
}

func ConnFunc(onReconnect func(error), onRetry func(error)) radix.ConnFunc {
	return func(network, addr string) (radix.Conn, error) {
		return Dial(&DialConfig{
			Network: network,
			Addr:    addr,

			OnReconnect: onReconnect,
			OnRetry:     onRetry,
		})
	}
}

func (rc *retryableRedisConn) Reconnect(cause error) error {
	if rc.inner != nil {
		rc.inner.Close()

	}

	if rc.conf.OnReconnect != nil {
		rc.conf.OnReconnect(cause)
	}

	inner, err := radix.Dial(rc.conf.Network, rc.conf.Addr, rc.conf.DialOpts...)
	rc.inner = inner
	return err
}

func (rc *retryableRedisConn) ReconnectLoop(cause error) error {
	for {
		err := rc.Reconnect(cause)
		if err == nil {
			return nil
		}

		// update cause
		cause = err
		time.Sleep(time.Millisecond * 500)
	}
}

// Do performs an Action, returning any error.
func (rc *retryableRedisConn) Do(a radix.Action) error {
	for {

		err := rc.inner.Do(a)
		if err == nil {
			return nil
		}

		// reconnect on io errors
		if _, ok := err.(net.Error); ok {
			rc.ReconnectLoop(err)
			continue
		}

		// retry on loading errors
		if strings.HasPrefix(err.Error(), "LOADING") {
			if rc.conf.OnRetry != nil {
				rc.conf.OnRetry(err)
			}
			time.Sleep(time.Millisecond * 250)
			continue
		}

		return err
	}
}

// Once Close() is called all future method calls on the Client will return
// an error
func (rc *retryableRedisConn) Close() error {
	return rc.inner.Close()
}

func (rc *retryableRedisConn) Encode(m resp.Marshaler) error {
	return rc.inner.Encode(m)
}

func (rc *retryableRedisConn) Decode(um resp.Unmarshaler) error {
	return rc.inner.Decode(um)
}

// Returns the underlying network connection, as-is. Read, Write, and Close
// should not be called on the returned Conn.
func (rc *retryableRedisConn) NetConn() net.Conn {
	return rc.inner.NetConn()
}

func FlatCmd(rcv interface{}, cmd, key string, args ...interface{}) radix.CmdAction {
	retryableCmd := &RetryableFlatCmd{
		rcv:  rcv,
		cmd:  cmd,
		key:  key,
		args: args,

		inner: radix.FlatCmd(rcv, cmd, key, args...),
	}

	return retryableCmd
}

type RetryableFlatCmd struct {
	rcv  interface{}
	cmd  string
	key  string
	args []interface{}

	inner radix.CmdAction
}

func (r *RetryableFlatCmd) getInner() radix.CmdAction {
	if r.inner == nil {
		r.inner = radix.FlatCmd(r.rcv, r.cmd, r.key, r.args...)
	}

	return r.inner
}

func (r *RetryableFlatCmd) Keys() []string {
	return r.getInner().Keys()
}

func (r *RetryableFlatCmd) Run(conn radix.Conn) error {
	if err := conn.Encode(r); err != nil {
		return err
	}

	return conn.Decode(r)
}

func (c *RetryableFlatCmd) MarshalRESP(w io.Writer) error {
	return c.getInner().MarshalRESP(w)
}

func (c *RetryableFlatCmd) UnmarshalRESP(br *bufio.Reader) error {
	err := c.getInner().UnmarshalRESP(br)

	// can't reuse the action after this
	c.inner = nil

	return err
}

func Cmd(rcv interface{}, cmd string, args ...string) radix.CmdAction {
	retryableCmd := &RetryableCmd{
		rcv:  rcv,
		cmd:  cmd,
		args: args,

		inner: radix.Cmd(rcv, cmd, args...),
	}

	return retryableCmd
}

type RetryableCmd struct {
	rcv  interface{}
	cmd  string
	key  string
	args []string

	inner radix.CmdAction
}

func (r *RetryableCmd) getInner() radix.CmdAction {
	if r.inner == nil {
		r.inner = radix.Cmd(r.rcv, r.cmd, r.args...)
	}

	return r.inner
}

func (r *RetryableCmd) Keys() []string {
	return r.getInner().Keys()
}

func (r *RetryableCmd) Run(conn radix.Conn) error {
	if err := conn.Encode(r); err != nil {
		return err
	}
	return conn.Decode(r)
}

func (c *RetryableCmd) MarshalRESP(w io.Writer) error {
	return c.getInner().MarshalRESP(w)
}

func (c *RetryableCmd) UnmarshalRESP(br *bufio.Reader) error {
	err := c.getInner().UnmarshalRESP(br)

	// can't reuse the action after this
	c.inner = nil

	return err
}
