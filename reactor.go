// +build linux

package reactor

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	ErrTooBigMsg = errors.New("too big msg")
)

type (
	Reactor struct {
		options *_Options

		addr      string
		listeners []net.Listener

		eventLoops []*_EventLoop
		waitGroup  sync.WaitGroup

		shutdownChan chan struct{}
	}
)

func New(addr string, opts ...Option) *Reactor {
	options := newOptions(opts...)
	r := &Reactor{
		options:      options,
		addr:         addr,
		eventLoops:   make([]*_EventLoop, 0, options.eventLoopSize),
		shutdownChan: make(chan struct{}),
	}
	for i := 0; i < cap(r.eventLoops); i++ {
		r.eventLoops = append(r.eventLoops, r.newEventLoop())
	}
	return r
}

func (r *Reactor) newListener(nonblocking bool) int {
	var lc net.ListenConfig
	if r.options.reusePort {
		lc.Control = func(network, address string, c syscall.RawConn) (err1 error) {
			err2 := c.Control(func(fd uintptr) {
				err1 = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			})
			if err1 != nil {
				return err1
			}
			return err2
		}
	}
	listener, err := lc.Listen(context.TODO(), "tcp", r.addr)
	_panic(err)

	fdValue := reflect.Indirect(reflect.ValueOf(listener)).FieldByName("fd")
	pfdValue := reflect.Indirect(fdValue).FieldByName("pfd")
	listenerFD := int(pfdValue.FieldByName("Sysfd").Int())
	if !nonblocking {
		_panic(syscall.SetNonblock(listenerFD, nonblocking))
	}

	r.listeners = append(r.listeners, listener)
	return listenerFD
}

func (r *Reactor) ListenAndServe() error {
	if r.options.reusePort {
		<-r.shutdownChan
		return net.ErrClosed
	}

	listenerFD := r.newListener(false)
	for {
		conn, err := r.accept(listenerFD)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				r.options.logErrorFunc("ListenAndServe", "err", err)
				time.Sleep(time.Second)
				continue
			}
			return err
		}
		r.logError("registerConn", r.registerConn(conn))
	}
}

func (r *Reactor) registerConn(c *Conn) error {
	leastEventLoop := r.eventLoops[0]
	for i := 1; i < len(r.eventLoops); i++ {
		if atomic.LoadInt32(&r.eventLoops[i].connSize) < atomic.LoadInt32(&leastEventLoop.connSize) {
			leastEventLoop = r.eventLoops[i]
		}
	}
	c.eventLoop = leastEventLoop
	return c.eventLoop.transferToTaskChan(func() {
		c.eventLoop.registerConn(c)
	})
}

func (r *Reactor) accept(listenerFD int) (*Conn, error) {
	fd, remoteAddr, err := syscall.Accept4(listenerFD, syscall.SOCK_NONBLOCK)
	if err != nil {
		return nil, &net.OpError{Op: "accept", Net: "tcp", Source: nil, Addr: r.listeners[0].Addr(), Err: err}
	}
	r.logError("SetsockoptInt", syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1))
	c := &Conn{
		fd:           fd,
		reactor:      r,
		onlyCallback: r.options.onlyCallback,
		remoteAddr:   toAddr(remoteAddr),
		readBuf:      make([]byte, r.options.readBufSize),
	}
	return c, nil
}

func (r *Reactor) Shutdown() {
	r.ShutdownListener()
	r.ShutdownEventLoop()
}

func (r *Reactor) ShutdownListener() {
	close(r.shutdownChan)
	for _, listener := range r.listeners {
		_ = listener.Close()
	}
}

func (r *Reactor) ShutdownEventLoop() {
	for _, subReactor := range r.eventLoops {
		subReactor.shutdown()
	}
	r.waitGroup.Wait()
}

func (r *Reactor) IsShutdown() bool {
	select {
	case <-r.shutdownChan:
		return true
	default:
		return false
	}
}

func (r *Reactor) logError(msg string, err error) {
	if r.notNeedLog(err) {
		return
	}
	r.options.logErrorFunc(msg, "err", err)
}

func (r *Reactor) notNeedLog(err error) bool {
	if err == nil || r.IsShutdown() {
		return true
	}
	if isNetError(err) && !r.options.debugMode {
		return true
	}
	return false
}

func isNetError(err error) bool {
	if err == io.EOF || err == ErrTooBigMsg {
		return true
	}
	_, ok := err.(net.Error)
	return ok
}

func (r *Reactor) logSessionError(msg string, err error, conn *Conn) {
	if r.notNeedLog(err) {
		return
	}
	r.options.logErrorFunc(msg, "err", err, "remote_addr", conn.RemoteAddr().String(), "session", conn.Session)
}

func toAddr(sa syscall.Sockaddr) net.Addr {
	switch sa := sa.(type) {
	case *syscall.SockaddrInet4:
		return &net.TCPAddr{IP: sa.Addr[:], Port: sa.Port}
	case *syscall.SockaddrInet6:
		return &net.TCPAddr{IP: sa.Addr[:], Port: sa.Port}
	case *syscall.SockaddrUnix:
		return &net.UnixAddr{Net: "unix", Name: sa.Name}
	}
	return nil
}
