package main

import (
	"encoding/binary"
	"github.com/go-productive/reactor"
	"net/http"
	_ "net/http/pprof"
	"strconv"
)

func main() {
	go func() {
		panic(http.ListenAndServe(":64001", nil))
	}()
	r := reactor.New(":64000",
		reactor.WithOnReadMsgFunc(func(reqBytes []byte, conn *reactor.Conn) {
			req, err := strconv.Atoi(string(reqBytes))
			if err != nil {
				panic(err)
			}
			rsp := strconv.Itoa(req + 1)
			bs := make([]byte, 2+len(rsp))
			binary.BigEndian.PutUint16(bs[:2], uint16(len(rsp)))
			copy(bs[2:], rsp)
			conn.SyncWrite(bs)
		}),
	)
	panic(r.ListenAndServe())
}
