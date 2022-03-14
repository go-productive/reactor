package main

import (
	"encoding/binary"
	"github.com/go-productive/reactor"
	"strconv"
)

func main() {
	mainReactor := reactor.New(":64000",
		reactor.WithOnReadMsgFunc(func(reqBytes []byte, conn *reactor.Conn) {
			req, err := strconv.Atoi(string(reqBytes))
			if err != nil {
				panic(err)
			}
			go func() {
				rsp := strconv.Itoa(req + 1)
				bs := make([]byte, 2+len(rsp))
				binary.BigEndian.PutUint16(bs[:2], uint16(len(rsp)))
				copy(bs[2:], rsp)
				if err := conn.AsyncWrite(bs); err != nil {
					panic(err)
				}
			}()
		}),
	)
	panic(mainReactor.ListenAndServe())
}
