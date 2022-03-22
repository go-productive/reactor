package main

import (
	"github.com/go-productive/reactor"
)

func main() {
	r := reactor.New(":64000",
		reactor.WithDebugMode(true),
		reactor.WithOnlyCallback(true),
		reactor.WithOnReadMsgFunc(func(reqBytes []byte, conn *reactor.Conn) {
			conn.SyncWrite(reqBytes)

			bs := append([]byte(nil), reqBytes...)
			go func() {
				if err := conn.AsyncWrite(bs); err != nil {
					panic(err)
				}
			}()
		}),
	)
	panic(r.ListenAndServe())
}
