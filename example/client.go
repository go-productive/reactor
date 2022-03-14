package main

import (
	"io"
	"net"
	"os"
	"time"
)

func main() {
	conn, err := net.Dial("tcp", ":64000")
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			if _, err := conn.Write([]byte(time.Now().String() + "\n")); err != nil {
				panic(err)
			}
			time.Sleep(time.Second)
		}
	}()
	if _, err := io.Copy(os.Stdout, conn); err != nil {
		panic(err)
	}
}
