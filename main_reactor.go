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
	ErrTooBigPackage = errors.New("too big package")
)

type (
	MainReactor struct {
		options *_Options

		listener   *net.TCPListener
		listenerFD int

		subReactors []*_SubReactor
		waitGroup   sync.WaitGroup

		shutdownChan chan struct{}
	}
)

func New(addr string, opts ...Option) *MainReactor {
	options := newOptions(opts...)
	m := &MainReactor{
		options:      options,
		subReactors:  make([]*_SubReactor, 0, options.subReactorSize),
		shutdownChan: make(chan struct{}),
	}
	m.initListener(addr)
	for i := 0; i < cap(m.subReactors); i++ {
		m.subReactors = append(m.subReactors, m.newSubReactor())
	}
	return m
}

func (m *MainReactor) initListener(addr string) {
	listener, err := net.Listen("tcp", addr)
	_panic(err)
	m.listener = listener.(*net.TCPListener)

	fdValue := reflect.Indirect(reflect.ValueOf(m.listener)).FieldByName("fd")
	pfdValue := reflect.Indirect(fdValue).FieldByName("pfd")
	m.listenerFD = int(pfdValue.FieldByName("Sysfd").Int())
	_panic(syscall.SetNonblock(m.listenerFD, false))
}

func (m *MainReactor) ListenAndServe() error {
	for {
		fd, remoteAddr, err := syscall.Accept4(m.listenerFD, syscall.SOCK_NONBLOCK)
		if err != nil {
			if m.IsShutdown() {
				return nil
			}
			opError := &net.OpError{Op: "accept", Net: "tcp", Source: nil, Addr: m.listener.Addr(), Err: err}
			if opError.Temporary() {
				time.Sleep(time.Second)
				m.options.logErrorFunc("ListenAndServe", "err", opError)
				continue
			}
			return opError
		}
		m.logError("SetsockoptInt", syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1))
		m.logError("registerConn", m.registerConn(fd, toAddr(remoteAddr)))
	}
}

func (m *MainReactor) Shutdown() {
	close(m.shutdownChan)
	_ = m.listener.Close()
	for _, subReactor := range m.subReactors {
		subReactor.shutdown()
	}
	m.waitGroup.Wait()
}

func (m *MainReactor) IsShutdown() bool {
	select {
	case <-m.shutdownChan:
		return true
	default:
		return false
	}
}

func (m *MainReactor) logError(msg string, err error) {
	if m.notNeedLog(err) {
		return
	}
	m.options.logErrorFunc(msg, "err", err)
}

func (m *MainReactor) notNeedLog(err error) bool {
	if err == nil || m.IsShutdown() {
		return true
	}
	if isNetError(err) && !m.options.debugMode {
		return true
	}
	return false
}

func isNetError(err error) bool {
	if err == io.EOF || err == ErrTooBigPackage {
		return true
	}
	_, ok := err.(net.Error)
	return ok
}

func (m *MainReactor) logSessionError(msg string, err error, conn *Conn) {
	if m.notNeedLog(err) {
		return
	}
	m.options.logErrorFunc(msg, "err", err, "remote_addr", conn.RemoteAddr().String(), "session", conn.Session)
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
