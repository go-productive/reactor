// +build linux

package reactor

import (
	"context"
	"golang.org/x/sys/unix"
	"net"
	"reflect"
	"runtime"
	"sync/atomic"
	"syscall"
	"unsafe"
)

type (
	_EventLoop struct {
		reactor      *Reactor
		epollFD      int
		listener     net.Listener
		listenerConn *Conn
		wakeConn     *Conn
		waked        uint32
		taskChan     chan func()

		connSet map[*Conn]struct{}

		events              [1024]epollevent
		pendingReadConnSet  map[*Conn]struct{}
		pendingWriteConnSet map[*Conn]struct{}
	}
)

const (
	eventsRead  = syscall.EPOLLIN | syscall.EPOLLRDHUP
	eventsWrite = syscall.EPOLLOUT
)

func (r *Reactor) newEventLoop() *_EventLoop {
	epollFD, err := syscall.EpollCreate1(0)
	_panic(err)
	wakeFD, err := unix.Eventfd(0, syscall.O_NONBLOCK)
	_panic(err)
	e := &_EventLoop{
		reactor:             r,
		epollFD:             epollFD,
		wakeConn:            &Conn{fd: wakeFD},
		taskChan:            make(chan func(), 1_0000),
		connSet:             make(map[*Conn]struct{}),
		pendingReadConnSet:  make(map[*Conn]struct{}),
		pendingWriteConnSet: make(map[*Conn]struct{}),
	}
	e.initListener(r.addr)
	_panic(e.epollCtlConn(syscall.EPOLL_CTL_ADD, e.listenerConn, syscall.EPOLLIN))
	_panic(e.epollCtlConn(syscall.EPOLL_CTL_ADD, e.wakeConn, syscall.EPOLLIN|unix.EPOLLET))
	r.waitGroup.Add(1)
	go e.eventLoop()
	return e
}

func (e *_EventLoop) initListener(addr string) {
	lc := &net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) (err1 error) {
			err2 := c.Control(func(fd uintptr) {
				err1 = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			})
			if err1 != nil {
				return err1
			}
			return err2
		},
	}
	listener, err := lc.Listen(context.TODO(), "tcp", addr)
	_panic(err)
	fdValue := reflect.Indirect(reflect.ValueOf(listener)).FieldByName("fd")
	pfdValue := reflect.Indirect(fdValue).FieldByName("pfd")
	listenerFD := int(pfdValue.FieldByName("Sysfd").Int())
	e.listener, e.listenerConn = listener, &Conn{fd: listenerFD}
}

func (e *_EventLoop) epollCtlConn(op int, conn *Conn, events uint32) error {
	event := &epollevent{
		events: events,
	}
	*(**Conn)(unsafe.Pointer(&event.data)) = conn
	return epollCtl(e.epollFD, op, conn.fd, event)
}

func (e *_EventLoop) eventLoop() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	defer e.reactor.waitGroup.Done()
	defer syscall.Close(e.epollFD)
	defer e.listener.Close()
	defer syscall.Close(e.wakeConn.fd)
	defer func() {
		for conn := range e.connSet {
			e.closeConn(conn)
		}
	}()

	for msec := -1; ; {
		n, err := e.epollWaitAndHandle(msec)
		_panic(err)
		if e.reactor.IsShutdown() {
			return
		}

		e.consumeTaskChan()
		e.handlePending()

		atomic.StoreUint32(&e.waked, 0)
		if n > 0 || len(e.taskChan) > 0 || len(e.pendingWriteConnSet) > 0 || len(e.pendingReadConnSet) > 0 {
			msec = 0
		} else {
			msec = -1
		}
	}
}

func (e *_EventLoop) epollWaitAndHandle(msec int) (int, error) {
	n, err := epollWait(e.epollFD, e.events[:], msec)
	if err != nil && err != syscall.EINTR {
		return 0, err
	}
	for i := 0; i < n; i++ {
		event := e.events[i]
		conn := *(**Conn)(unsafe.Pointer(&event.data))
		if conn.fd == e.wakeConn.fd {
			continue
		}
		if conn.fd == e.listenerConn.fd {
			if err := e.handleAccept(); err != nil {
				return 0, err
			}
			continue
		}
		if event.events&eventsRead != 0 {
			if !e.handleRead(conn) {
				e.pendingReadConnSet[conn] = struct{}{}
			}
		}
		if event.events&eventsWrite != 0 && !conn.writeBuf.isEmpty() {
			if !e.writeBufToFD(conn) {
				e.pendingWriteConnSet[conn] = struct{}{}
			}
		}
	}
	return n, nil
}

func (e *_EventLoop) handleAccept() error {
	c, err := e.accept()
	if err != nil {
		opError := &net.OpError{Op: "accept", Net: "tcp", Source: nil, Addr: e.listener.Addr(), Err: err}
		if opError.Temporary() {
			return nil
		}
		return opError
	}
	e.reactor.options.onConnFunc(c)
	e.connSet[c] = struct{}{}
	return nil
}

func (e *_EventLoop) accept() (c *Conn, err error) {
	fd, remoteAddr, err := syscall.Accept4(e.listenerConn.fd, syscall.SOCK_NONBLOCK)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = syscall.Close(fd)
		}
	}()
	_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)

	c = &Conn{
		fd:           fd,
		eventLoop:    e,
		onlyCallback: e.reactor.options.onlyCallback,
		remoteAddr:   toAddr(remoteAddr),
		readBuf:      make([]byte, e.reactor.options.readBufSize),
	}
	return c, e.epollCtlConn(syscall.EPOLL_CTL_ADD, c, eventsRead|eventsWrite|unix.EPOLLET)
}

func (e *_EventLoop) handlePending() {
	for conn := range e.pendingWriteConnSet {
		if e.writeBufToFD(conn) {
			delete(e.pendingWriteConnSet, conn)
		}
	}
	for conn := range e.pendingReadConnSet {
		if e.handleRead(conn) {
			delete(e.pendingReadConnSet, conn)
		}
	}
}

func (e *_EventLoop) writeBufToFD(conn *Conn) bool {
	writeFull, err := conn.writeBufToFD()
	if err != nil {
		e.reactor.logError("writeBufToFD", err, conn)
		e.closeConn(conn)
		return true
	}
	return writeFull
}

func (e *_EventLoop) handleRead(conn *Conn) bool {
	edgeReadAll, messages, err := conn.readMsg()
	if err != nil {
		e.reactor.logError("handleRead", err, conn)
		e.closeConn(conn)
		return true
	}
	onlyCallback := conn.onlyCallback
	for _, msg := range messages {
		e.reactor.options.onReadMsgFunc(msg, conn)
	}
	if onlyCallback != conn.onlyCallback {
		return false
	}
	return edgeReadAll
}

func (e *_EventLoop) consumeTaskChan() {
	for {
		select {
		case fn := <-e.taskChan:
			fn()
		default:
			return
		}
	}
}

func (e *_EventLoop) transferToTaskChan(fn func()) error {
	select {
	case <-e.reactor.shutdownChan:
		return net.ErrClosed
	case e.taskChan <- fn:
		return e.wakeup()
	}
}

var (
	u         uint64 = 1
	wakeBytes        = (*(*[8]byte)(unsafe.Pointer(&u)))[:]
)

func (e *_EventLoop) wakeup() error {
	if !atomic.CompareAndSwapUint32(&e.waked, 0, 1) {
		return nil
	}
	for {
		_, err := syscall.Write(e.wakeConn.fd, wakeBytes)
		if err != syscall.EINTR && err != syscall.EAGAIN {
			return err
		}
	}
}

func (e *_EventLoop) write(conn *Conn, bs []byte) {
	conn.writeBuf.write(bs)
	if !e.writeBufToFD(conn) {
		e.pendingWriteConnSet[conn] = struct{}{}
	}
}

func (e *_EventLoop) closeConn(conn *Conn) {
	if conn.closed {
		return
	}
	e.reactor.logError("closeConn", e.epollCtlConn(syscall.EPOLL_CTL_DEL, conn, 0), conn)
	e.reactor.logError("closeConn", conn.newOpError("close", syscall.Close(conn.fd)), conn)
	delete(e.connSet, conn)
	delete(e.pendingReadConnSet, conn)
	delete(e.pendingWriteConnSet, conn)
	e.reactor.options.onDisConnFunc(conn)
	conn.closed = true
	return
}
