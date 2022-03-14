// +build linux

package reactor

import (
	"golang.org/x/sys/unix"
	"net"
	"runtime"
	"sync/atomic"
	"syscall"
	"unsafe"
)

type (
	_SubReactor struct {
		mainReactor *MainReactor
		epollFD     int

		wakeConn *Conn
		waked    uint32
		taskChan chan func()

		connSet  map[*Conn]struct{}
		connSize int32

		events              [1024]epollevent
		pendingReadConnSet  map[*Conn]struct{}
		pendingWriteConnSet map[*Conn]struct{}
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

func (m *MainReactor) newSubReactor() *_SubReactor {
	epollFD, err := syscall.EpollCreate1(0)
	_panic(err)
	s := &_SubReactor{
		mainReactor:         m,
		epollFD:             epollFD,
		taskChan:            make(chan func(), 1_0000),
		connSet:             make(map[*Conn]struct{}),
		pendingReadConnSet:  make(map[*Conn]struct{}),
		pendingWriteConnSet: make(map[*Conn]struct{}),
	}
	r1, _, errNo := syscall.Syscall(syscall.SYS_EVENTFD2, 0, syscall.O_NONBLOCK, 0)
	if errNo != 0 {
		_panic(errNo)
	}
	s.wakeConn = &Conn{fd: int(r1)}
	_panic(s.epollCtlConn(syscall.EPOLL_CTL_ADD, s.wakeConn, syscall.EPOLLIN|unix.EPOLLET))
	m.waitGroup.Add(1)
	go s.eventLoop()
	return s
}

func (s *_SubReactor) epollCtlConn(op int, conn *Conn, events uint32) error {
	event := &epollevent{
		events: events,
	}
	*(**Conn)(unsafe.Pointer(&event.data)) = conn
	return epollCtl(s.epollFD, op, conn.fd, event)
}

func (s *_SubReactor) eventLoop() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer s.mainReactor.waitGroup.Done()
	for msec := -1; ; {
		n, err := s.epollWaitAndHandle(msec)
		if err != nil {
			s.mainReactor.logError("epollWaitAndHandle", err)
			for conn := range s.connSet {
				s.mainReactor.logSessionError("closeConn", s.closeConn(conn), conn)
			}
			if s.mainReactor.IsShutdown() {
				return
			}
			panic(err)
		}

		s.consumeTaskChan()
		s.handlePending()

		atomic.StoreUint32(&s.waked, 0)
		if n > 0 || len(s.taskChan) > 0 || len(s.pendingWriteConnSet) > 0 || len(s.pendingReadConnSet) > 0 {
			msec = 0
		} else {
			msec = -1
		}
	}
}

func (s *_SubReactor) handlePending() {
	for conn := range s.pendingWriteConnSet {
		if writeFull, err := conn.writeBufToFD(); err != nil {
			s.mainReactor.logSessionError("writeBufToFD", err, conn)
			s.mainReactor.logSessionError("closeConn", s.closeConn(conn), conn)
		} else if writeFull {
			delete(s.pendingWriteConnSet, conn)
		}
	}
	for conn := range s.pendingReadConnSet {
		if edgeReadAll, err := s.handleRead(conn); err != nil {
			s.mainReactor.logSessionError("handleRead", err, conn)
			s.mainReactor.logSessionError("closeConn", s.closeConn(conn), conn)
		} else if edgeReadAll {
			delete(s.pendingReadConnSet, conn)
		}
	}
}

func (s *_SubReactor) handleRead(conn *Conn) (bool, error) {
	edgeReadAll, messages, err := conn.readMsg()
	if err != nil {
		return edgeReadAll, err
	}
	onlyCallback := conn.onlyCallback
	for _, msg := range messages {
		s.mainReactor.options.onReadMsgFunc(msg, conn)
	}
	if onlyCallback != conn.onlyCallback {
		return false, nil
	}
	return edgeReadAll, nil
}

func (s *_SubReactor) epollWaitAndHandle(msec int) (int, error) {
	n, err := epollWait(s.epollFD, s.events[:], msec)
	if err != nil && err != syscall.EINTR {
		return n, err
	}
	for i := 0; i < n; i++ {
		event := s.events[i]
		conn := *(**Conn)(unsafe.Pointer(&event.data))
		if conn.fd == s.wakeConn.fd {
			continue
		}
		if event.events&eventsRead != 0 {
			if edgeReadAll, err := s.handleRead(conn); err != nil {
				s.mainReactor.logSessionError("handleRead", err, conn)
				s.mainReactor.logSessionError("closeConn", s.closeConn(conn), conn)
			} else if !edgeReadAll {
				s.pendingReadConnSet[conn] = struct{}{}
			}
		}
		if event.events&eventsWrite != 0 && !conn.writeBuf.isEmpty() {
			if writeFull, err := conn.writeBufToFD(); err != nil {
				s.mainReactor.logSessionError("writeBufToFD", err, conn)
				s.mainReactor.logSessionError("closeConn", s.closeConn(conn), conn)
			} else if !writeFull {
				s.pendingWriteConnSet[conn] = struct{}{}
			}
		}
	}
	return n, nil
}

func (s *_SubReactor) transferToTaskChan(fn func()) error {
	select {
	case <-s.mainReactor.shutdownChan:
		return net.ErrClosed
	case s.taskChan <- fn:
		return s.wakeup()
	}
}

func (s *_SubReactor) wakeup() error {
	if !atomic.CompareAndSwapUint32(&s.waked, 0, 1) {
		return nil
	}
	for {
		_, err := syscall.Write(s.wakeConn.fd, wakeBytes)
		if err != syscall.EINTR && err != syscall.EAGAIN {
			return err
		}
	}
}

func (s *_SubReactor) consumeTaskChan() {
	for {
		select {
		case fn := <-s.taskChan:
			fn()
		default:
			return
		}
	}
}

func (s *_SubReactor) registerConn(conn *Conn) {
	err := s.epollCtlConn(syscall.EPOLL_CTL_ADD, conn, eventsRead|eventsWrite|unix.EPOLLET)
	if err != nil {
		s.mainReactor.logError("registerConn", err)
		s.mainReactor.logError("registerConn", conn.newOpError("close", syscall.Close(conn.fd)))
		return
	}
	s.mainReactor.options.onConnFunc(conn)
	s.connSet[conn] = struct{}{}
	atomic.AddInt32(&s.connSize, 1)
}

func (s *_SubReactor) write(conn *Conn, bs []byte) {
	conn.writeBuf.write(bs)
	if writeFull, err := conn.writeBufToFD(); err != nil {
		s.mainReactor.logSessionError("writeBufToFD", err, conn)
		s.mainReactor.logSessionError("closeConn", s.closeConn(conn), conn)
	} else if !writeFull {
		s.pendingWriteConnSet[conn] = struct{}{}
	}
}

func (s *_SubReactor) shutdown() {
	s.mainReactor.logError("Close", syscall.Close(s.epollFD))
	s.mainReactor.logError("Close", syscall.Close(s.wakeConn.fd))
}

func (s *_SubReactor) closeConn(conn *Conn) error {
	if conn.closed {
		return nil
	}
	s.mainReactor.logSessionError("closeConn", conn.newOpError("close", syscall.Close(conn.fd)), conn)
	err := s.epollCtlConn(syscall.EPOLL_CTL_DEL, conn, 0)
	if err != nil && err != syscall.EBADF {
		s.mainReactor.logSessionError("closeConn", err, conn)
	}
	delete(s.connSet, conn)
	atomic.AddInt32(&s.connSize, -1)
	delete(s.pendingReadConnSet, conn)
	delete(s.pendingWriteConnSet, conn)
	s.mainReactor.options.onDisConnFunc(conn)
	conn.closed = true
	return nil
}
