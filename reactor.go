// +build linux

package reactor

import (
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
)

var (
	ErrTooBigMsg = errors.New("too big msg")
)

type (
	Reactor struct {
		options      *_Options
		eventLoops   []*_EventLoop
		waitGroup    sync.WaitGroup
		shutdownChan chan struct{}
	}
)

func New(addr string, opts ...Option) *Reactor {
	options := newOptions(opts...)
	r := &Reactor{
		options:      options,
		eventLoops:   make([]*_EventLoop, 0, options.eventLoopSize),
		shutdownChan: make(chan struct{}),
	}
	for i := 0; i < cap(r.eventLoops); i++ {
		r.eventLoops = append(r.eventLoops, r.newEventLoop(addr))
	}
	return r
}

func (r *Reactor) ListenAndServe() error {
	<-r.shutdownChan
	return net.ErrClosed
}

func (r *Reactor) Shutdown() {
	close(r.shutdownChan)
	for _, eventLoop := range r.eventLoops {
		eventLoop.shutdown()
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

func (r *Reactor) logError(msg string, err error, conn *Conn) {
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
