// +build linux

package reactor

import (
	"golang.org/x/sys/unix"
	"syscall"
	"unsafe"
)

type epollevent struct {
	events uint32
	data   [8]byte // unaligned uintptr
}

// THIS FILE IS GENERATED BY THE COMMAND AT THE TOP; DO NOT EDIT

func epollCtl(epfd int, op int, fd int, event *epollevent) (err error) {
	_, _, e1 := syscall.RawSyscall6(syscall.SYS_EPOLL_CTL, uintptr(epfd), uintptr(op), uintptr(fd), uintptr(unsafe.Pointer(event)), 0, 0)
	if e1 != 0 {
		err = errnoErr(e1)
	}
	return
}

func epollWait(epfd int, events []epollevent, msec int) (int, error) {
	ep := unsafe.Pointer(&events[0])
	var (
		n     uintptr
		errno unix.Errno
	)
	if msec == 0 { // non-block system call, use RawSyscall6 to avoid getting preempted by runtime
		n, _, errno = syscall.RawSyscall6(syscall.SYS_EPOLL_WAIT, uintptr(epfd), uintptr(ep), uintptr(len(events)), uintptr(msec), 0, 0)
	} else {
		n, _, errno = syscall.Syscall6(syscall.SYS_EPOLL_WAIT, uintptr(epfd), uintptr(ep), uintptr(len(events)), uintptr(msec), 0, 0)
	}
	if errno != 0 {
		return int(n), errnoErr(errno)
	}
	return int(n), nil
}

// Do the interface allocations only once for common
// Errno values.
var (
	errEAGAIN error = syscall.EAGAIN
	errEINVAL error = syscall.EINVAL
	errENOENT error = syscall.ENOENT
)

// errnoErr returns common boxed Errno values, to prevent
// allocations at runtime.
func errnoErr(e syscall.Errno) error {
	switch e {
	case 0:
		return nil
	case syscall.EAGAIN:
		return errEAGAIN
	case syscall.EINVAL:
		return errEINVAL
	case syscall.ENOENT:
		return errENOENT
	}
	return e
}
