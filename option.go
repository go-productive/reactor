// +build linux

package reactor

import (
	"encoding/binary"
	"math"
	"runtime"
)

type (
	Logger interface {
		Errorw(msg string, keysAndValues ...interface{})
	}
	_Options struct {
		subReactorSize int
		debugMode      bool

		readBufSize   uint16
		onlyCallback  bool
		headLen       int
		headLenFunc   func(bs []byte) int
		maxBodyLength int

		onConnFunc    func(conn *Conn)
		onDisConnFunc func(conn *Conn)
		onReadMsgFunc func(reqBytes []byte, conn *Conn)
	}
	Option func(*_Options)
)

func newOptions(opts ...Option) *_Options {
	o := &_Options{
		subReactorSize: runtime.GOMAXPROCS(0),
		readBufSize:    1 << 10, // 1K
		headLen:        2,
		headLenFunc: func(bs []byte) int {
			return int(binary.BigEndian.Uint16(bs))
		},
		maxBodyLength: math.MaxUint16,

		onConnFunc: func(conn *Conn) {
		},
		onDisConnFunc: func(conn *Conn) {
		},
		onReadMsgFunc: func(reqBytes []byte, conn *Conn) {
		},
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func WithSubReactorSize(subReactorSize int) Option {
	return func(o *_Options) {
		o.subReactorSize = subReactorSize
	}
}

func WithDebugMode(debugMode bool) Option {
	return func(o *_Options) {
		o.debugMode = debugMode
	}
}

func WithReadBufSize(readBufSize uint16) Option {
	return func(o *_Options) {
		o.readBufSize = readBufSize
	}
}

func WithOnlyCallback(onlyCallback bool) Option {
	return func(o *_Options) {
		o.onlyCallback = onlyCallback
	}
}

func WithHeadLen(headLen int, headLenFunc func(bs []byte) int, maxBodyLength int) Option {
	return func(o *_Options) {
		o.headLen = headLen
		o.headLenFunc = headLenFunc
		o.maxBodyLength = maxBodyLength
	}
}

func WithOnConn(onConnFunc func(conn *Conn)) Option {
	return func(o *_Options) {
		o.onConnFunc = onConnFunc
	}
}

func WithOnDisConn(onDisConnFunc func(conn *Conn)) Option {
	return func(o *_Options) {
		o.onDisConnFunc = onDisConnFunc
	}
}

func WithReadMsgFunc(onReadMsgFunc func(reqBytes []byte, conn *Conn)) Option {
	return func(o *_Options) {
		o.onReadMsgFunc = onReadMsgFunc
	}
}

func _panic(err error) {
	if err != nil {
		panic(err)
	}
}
