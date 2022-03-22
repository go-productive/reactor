// +build linux

package reactor

import (
	"errors"
	"io"
	"net"
	"reflect"
	"sync"
	"syscall"
	"time"
)

var (
	ErrTooBigMsg = errors.New("too big msg")
)

type (
	Reactor struct {
		options *_Options

		listener   *net.TCPListener
		listenerFD int

		eventLoops []*_EventLoop
		waitGroup  sync.WaitGroup

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
	r.initListener(addr)
	for i := 0; i < cap(r.eventLoops); i++ {
		r.eventLoops = append(r.eventLoops, r.newEventLoop())
	}
	return r
}

func (r *Reactor) initListener(addr string) {
	listener, err := net.Listen("tcp", addr)
	_panic(err)
	r.listener = listener.(*net.TCPListener)

	fdValue := reflect.Indirect(reflect.ValueOf(r.listener)).FieldByName("fd")
	pfdValue := reflect.Indirect(fdValue).FieldByName("pfd")
	r.listenerFD = int(pfdValue.FieldByName("Sysfd").Int())
	_panic(syscall.SetNonblock(r.listenerFD, false))
}

func (r *Reactor) ListenAndServe() error {
	for {
		fd, remoteAddr, err := syscall.Accept4(r.listenerFD, syscall.SOCK_NONBLOCK)
		if err != nil {
			if r.IsShutdown() {
				return err
			}
			opError := &net.OpError{Op: "accept", Net: "tcp", Source: nil, Addr: r.listener.Addr(), Err: err}
			if opError.Temporary() {
				r.options.logErrorFunc("ListenAndServe", "err", opError)
				time.Sleep(time.Second)
				continue
			}
			return opError
		}
		r.logError("SetsockoptInt", syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1))
		r.logError("registerConn", r.registerConn(fd, toAddr(remoteAddr)))
	}
}

func (r *Reactor) Shutdown() {
	r.ShutdownListener()
	r.ShutdownEventLoop()
}

func (r *Reactor) ShutdownListener() {
	close(r.shutdownChan)
	_ = r.listener.Close()
}

func (r *Reactor) ShutdownEventLoop() {
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
