package main

import (
	"github.com/jonas747/retryableredis"
	"github.com/mediocregopher/radix"
	"log"
	"time"
)

func main() {
	rc, err := retryableredis.Dial(&retryableredis.DialConfig{
		Network: "tcp",
		Addr:    "localhost:6379",
		OnRetry: func(err error) {
			log.Println("Retry triggered: ", err)
		},
		OnReconnect: func(err error) {
			log.Println("Reconnect triggered: ", err)
		},
	})

	if err != nil {
		panic(err)
	}

	// fill(rc)

	for {

		var r int
		err := rc.Do(radix.Cmd(&r, "INCR", "test"))
		if err != nil {
			log.Println("ret, Err: ", err)
		}
		log.Println("r: ", r)

		time.Sleep(time.Millisecond * 100)
	}
}

func fill(rc radix.Conn) {
	for i := 0; i < 100000; i++ {
		rc.Do(radix.FlatCmd(nil, "HSET", "testing_h", i, "wew"))
	}
}
