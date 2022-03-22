// +build linux

package reactor

import (
	"golang.org/x/sys/unix"
	"net"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

type (
	_EventLoop struct {
		reactor *Reactor
		epollFD int

		wakeConn *Conn
		waked    uint32
		taskChan chan func()

		connSet  map[*Conn]struct{}
		connSize int32

		events              [1024]epollevent
		pendingReadConnSet  map[*Conn]struct{}
		pendingWriteConnSet map[*Conn]struct{}

		listenerConn *Conn
	}
)

const (
	eventsRead  = syscall.EPOLLIN | syscall.EPOLLRDHUP
	eventsWrite = syscall.EPOLLOUT
)

var (
	u         uint64 = 1
	wakeBytes        = (*(*[8]byte)(unsafe.Pointer(&u)))[:]
)

func (r *Reactor) newEventLoop() *_EventLoop {
	epollFD, err := syscall.EpollCreate1(0)
	_panic(err)
	wakeFD, err := unix.Eventfd(0, syscall.O_NONBLOCK)
	_panic(err)
	s := &_EventLoop{
		reactor:             r,
		epollFD:             epollFD,
		wakeConn:            &Conn{fd: wakeFD},
		taskChan:            make(chan func(), 1_0000),
		connSet:             make(map[*Conn]struct{}),
		pendingReadConnSet:  make(map[*Conn]struct{}),
		pendingWriteConnSet: make(map[*Conn]struct{}),
	}
	_panic(s.epollCtlConn(syscall.EPOLL_CTL_ADD, s.wakeConn, syscall.EPOLLIN|unix.EPOLLET))
	if r.options.reusePort {
		listenerFD := r.newListener(true)
		s.listenerConn = &Conn{fd: listenerFD}
		_panic(s.epollCtlConn(syscall.EPOLL_CTL_ADD, s.listenerConn, syscall.EPOLLIN))
	}
	r.waitGroup.Add(1)
	go s.eventLoop()
	return s
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
	for msec := -1; ; {
		n, err := e.epollWaitAndHandle(msec)
		if err != nil {
			e.consumeTaskChan()
			for ; len(e.connSet) > 0; time.Sleep(time.Millisecond) {
				for conn := range e.connSet {
					if !conn.writeBuf.isEmpty() {
						_, _ = e.writeBufToFD(conn)
					}
					if conn.writeBuf.isEmpty() {
						e.closeConn(conn)
					}
				}
			}
			if e.reactor.IsShutdown() {
				return
			}
			panic(err)
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

func (e *_EventLoop) writeBufToFD(conn *Conn) (bool, error) {
	writeFull, err := conn.writeBufToFD()
	if err != nil {
		e.reactor.logSessionError("writeBufToFD", err, conn)
		e.closeConn(conn)
	}
	return writeFull, err
}

func (e *_EventLoop) handlePending() {
	for conn := range e.pendingWriteConnSet {
		if writeFull, err := e.writeBufToFD(conn); err == nil && writeFull {
			delete(e.pendingWriteConnSet, conn)
		}
	}
	for conn := range e.pendingReadConnSet {
		if edgeReadAll, err := e.handleRead(conn); err != nil {
			e.reactor.logSessionError("handleRead", err, conn)
			e.closeConn(conn)
		} else if edgeReadAll {
			delete(e.pendingReadConnSet, conn)
		}
	}
}

func (e *_EventLoop) handleRead(conn *Conn) (bool, error) {
	edgeReadAll, messages, err := conn.readMsg()
	if err != nil {
		return edgeReadAll, err
	}
	onlyCallback := conn.onlyCallback
	for _, msg := range messages {
		e.reactor.options.onReadMsgFunc(msg, conn)
	}
	if onlyCallback != conn.onlyCallback {
		return false, nil
	}
	return edgeReadAll, nil
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
		if e.listenerConn != nil && conn.fd == e.listenerConn.fd {
			if err := e.handleAccept(); err != nil {
				return 0, err
			}
			continue
		}
		if event.events&eventsRead != 0 {
			if edgeReadAll, err := e.handleRead(conn); err != nil {
				e.reactor.logSessionError("handleRead", err, conn)
				e.closeConn(conn)
			} else if !edgeReadAll {
				e.pendingReadConnSet[conn] = struct{}{}
			}
		}
		if event.events&eventsWrite != 0 && !conn.writeBuf.isEmpty() {
			if writeFull, err := e.writeBufToFD(conn); err == nil && !writeFull {
				e.pendingWriteConnSet[conn] = struct{}{}
			}
		}
	}
	return n, nil
}

func (e *_EventLoop) handleAccept() error {
	for i := 0; i < 10; i++ {
		conn, err := e.reactor.accept(e.listenerConn.fd)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				return nil
			}
			return err
		}
		conn.eventLoop = e
		e.registerConn(conn)
	}
	return nil
}

func (e *_EventLoop) transferToTaskChan(fn func()) error {
	select {
	case <-e.reactor.shutdownChan:
		return net.ErrClosed
	case e.taskChan <- fn:
		return e.wakeup()
	}
}

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

func (e *_EventLoop) registerConn(conn *Conn) {
	err := e.epollCtlConn(syscall.EPOLL_CTL_ADD, conn, eventsRead|eventsWrite|unix.EPOLLET)
	if err != nil {
		e.reactor.logError("registerConn", err)
		e.reactor.logError("registerConn", conn.newOpError("close", syscall.Close(conn.fd)))
		return
	}
	e.reactor.options.onConnFunc(conn)
	e.connSet[conn] = struct{}{}
	atomic.AddInt32(&e.connSize, 1)
}

func (e *_EventLoop) write(conn *Conn, bs []byte) {
	conn.writeBuf.write(bs)
	if writeFull, err := e.writeBufToFD(conn); err == nil && !writeFull {
		e.pendingWriteConnSet[conn] = struct{}{}
	}
}

func (e *_EventLoop) shutdown() {
	e.reactor.logError("Close", syscall.Close(e.epollFD))
	e.reactor.logError("Close", syscall.Close(e.wakeConn.fd))
}

func (e *_EventLoop) closeConn(conn *Conn) {
	if conn.closed {
		return
	}
	e.reactor.logSessionError("closeConn", conn.newOpError("close", syscall.Close(conn.fd)), conn)
	err := e.epollCtlConn(syscall.EPOLL_CTL_DEL, conn, 0)
	if err != nil && err != syscall.EBADF {
		e.reactor.logSessionError("closeConn", err, conn)
	}
	delete(e.connSet, conn)
	atomic.AddInt32(&e.connSize, -1)
	delete(e.pendingReadConnSet, conn)
	delete(e.pendingWriteConnSet, conn)
	e.reactor.options.onDisConnFunc(conn)
	conn.closed = true
	return
}
