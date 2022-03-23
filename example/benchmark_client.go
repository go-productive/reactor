package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"time"
)

func main() {
	for i := 0; i < 50; i++ {
		go produce()
	}
	select {}
}

func produce() {
	conn, err := net.Dial("tcp", ":64000")
	if err != nil {
		panic(err)
	}
	ch := make(chan int, 1_0000)
	defer close(ch)
	go func() {
		defer conn.Close()

		headBuf := make([]byte, 2)
		for value := range ch {
			if _, err := io.ReadFull(conn, headBuf); err != nil {
				panic(err)
			}
			rspBytes := make([]byte, binary.BigEndian.Uint16(headBuf))
			if _, err := io.ReadFull(conn, rspBytes); err != nil {
				panic(err)
			}
			rsp, err := strconv.Atoi(string(rspBytes))
			if err != nil {
				panic(err)
			}
			if rsp != value+1 {
				panic(fmt.Sprintf("%v,%v", rsp, value))
			}

			time.Sleep(time.Millisecond)
		}
	}()
	for rand.Intn(10) != 0 {
		value := rand.Int()
		req := strconv.Itoa(value)
		reqBytes := make([]byte, 2+len(req))
		binary.BigEndian.PutUint16(reqBytes[:2], uint16(len(req)))
		copy(reqBytes[2:], req)
		if _, err := conn.Write(reqBytes); err != nil {
			panic(err)
		}
		ch <- value
	}
	go produce()
}
