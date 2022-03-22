// +build linux

package reactor

import (
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"syscall"
)

type (
	Conn struct {
		fd           int
		reactor      *Reactor
		eventLoop    *_EventLoop
		remoteAddr   net.Addr
		closed       bool
		onlyCallback bool

		writeBuf _LinkBuf

		readIndex      int
		msgIndex       int
		readBuf        []byte
		readBigBodyBuf []byte

		Session interface{}
	}
)

// AsyncWrite write bytes to conn in other goroutine.
func (c *Conn) AsyncWrite(bs []byte) error {
	return c.eventLoop.transferToTaskChan(func() {
		c.eventLoop.write(c, bs)
	})
}

// SyncWrite write bytes to conn.
// Thread-unsafe, call it in callback onReadMsgFunc
func (c *Conn) SyncWrite(bs []byte) {
	c.eventLoop.write(c, bs)
}

// CancelOnlyCallback go on splitting msg on tcp level by Conn self when WithOnlyCallback, it can be less byte copy.
//
// Thread-unsafe, call it in callback onReadMsgFunc
func (c *Conn) CancelOnlyCallback(sticky []byte) {
	if len(sticky) > len(c.readBuf) {
		panic(fmt.Sprintf("too big sticky, len(sticky):%v, len(c.readBuf):%v", len(sticky), len(c.readBuf)))
	}
	c.readIndex = copy(c.readBuf, sticky)
	c.onlyCallback = false
}

// Close closes the connection.
// Any blocked Read or Write operations will be unblocked and return errors.
func (c *Conn) Close() error {
	return c.eventLoop.transferToTaskChan(func() {
		c.eventLoop.closeConn(c)
	})
}

func (c *Conn) newOpError(op string, err error) error {
	if err == nil {
		return nil
	}
	return &net.OpError{Op: op, Net: "tcp", Source: c.LocalAddr(), Addr: c.remoteAddr, Err: err}
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr {
	return c.reactor.listeners[0].Addr()
}

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *Conn) readMsg() (bool, [][]byte, error) {
	if c.onlyCallback {
		edgeReadAll, readN, err := c.readToBuf(c.readBuf)
		return edgeReadAll, [][]byte{c.readBuf[:readN]}, err
	}
	if c.readBigBodyBuf != nil {
		return c.readBigBodyMsg()
	}

	if c.msgIndex > 0 {
		c.readIndex = copy(c.readBuf, c.readBuf[c.msgIndex:c.readIndex])
		c.msgIndex = 0
	}
	edgeReadAll, readN, err := c.readToBuf(c.readBuf[c.readIndex:])
	if err != nil {
		return edgeReadAll, nil, err
	}
	c.readIndex += readN

	var messages [][]byte
	options := c.reactor.options
	for start := c.msgIndex + options.headLen; c.readIndex >= start; start = c.msgIndex + options.headLen {
		bodyLength := options.headLenFunc(c.readBuf[c.msgIndex:start])
		if bodyLength > options.maxBodyLength {
			return false, nil, ErrTooBigMsg
		}
		if bodyLength > len(c.readBuf)-options.headLen {
			c.readBigBodyBuf = make([]byte, bodyLength)
			c.readIndex = copy(c.readBigBodyBuf, c.readBuf[start:c.readIndex])
			c.msgIndex = 0
			return c.readBigBodyMsg()
		}

		end := start + bodyLength
		if c.readIndex < end {
			return edgeReadAll, messages, nil
		}
		messages = append(messages, c.readBuf[start:end])
		c.msgIndex = end
	}
	return edgeReadAll, messages, nil
}

func (c *Conn) readBigBodyMsg() (bool, [][]byte, error) {
	edgeReadAll, readN, err := c.readToBuf(c.readBigBodyBuf[c.readIndex:])
	if err != nil {
		return edgeReadAll, nil, err
	}
	c.readIndex += readN
	if c.readIndex < len(c.readBigBodyBuf) {
		return edgeReadAll, nil, nil
	}
	messages := [][]byte{c.readBigBodyBuf}
	c.readBigBodyBuf = nil
	c.readIndex = 0
	return edgeReadAll, messages, nil
}

func (c *Conn) readToBuf(readBuf []byte) (bool, int, error) {
	readN := 0
	for {
		n, err := ignoringEINTRIO(func() (int, error) {
			return syscall.Read(c.fd, readBuf[readN:])
		})
		if err != nil && err != syscall.EAGAIN {
			return false, 0, c.newOpError("read", err)
		}
		if n == 0 && err == nil {
			return false, 0, io.EOF
		}
		if n > 0 {
			readN += n
		}
		edgeReadAll := err == syscall.EAGAIN
		if edgeReadAll || readN >= len(readBuf) {
			return edgeReadAll, readN, nil
		}
	}
}

func (c *Conn) writeBufToFD() (bool, error) {
	for {
		bss, total := c.writeBuf.bytes()
		writeN, err := ignoringEINTRIO(func() (int, error) {
			return unix.Writev(c.fd, bss)
		})
		if err != nil && err != syscall.EAGAIN {
			return false, c.newOpError("write", err)
		}
		if writeN > 0 {
			c.writeBuf.discard(writeN)
		}
		writeFull := writeN == total || err == syscall.EAGAIN
		if writeFull {
			return writeFull, nil
		}
		if writeN == 0 {
			return false, c.newOpError("write", io.ErrUnexpectedEOF)
		}
	}
}

// ignoringEINTRIO is like ignoringEINTR, but just for IO calls.
func ignoringEINTRIO(fn func() (int, error)) (int, error) {
	for {
		n, err := fn()
		if err != syscall.EINTR {
			return n, err
		}
	}
}
