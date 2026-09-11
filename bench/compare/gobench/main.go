// Minimal role-based bench peer, same contract as nimbench.nim.
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	nats "github.com/nats-io/nats.go"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	role := os.Args[1]
	url := os.Args[2]
	nc, err := nats.Connect(url, nats.NoReconnect())
	must(err)
	defer nc.Close()
	switch role {
	case "resp":
		_, err := nc.Subscribe(os.Args[3], func(m *nats.Msg) {
			if m.Reply != "" {
				nc.Publish(m.Reply, m.Data)
			}
		})
		must(err)
		must(nc.Flush())
		fmt.Println("READY")
		select {} // driver terminates us
	case "req":
		subject := os.Args[3]
		count, _ := strconv.Atoi(os.Args[4])
		payload := []byte(strings.Repeat("x", must2(os.Args[5])))
		start := time.Now()
		for i := 0; i < count; i++ {
			if _, err := nc.Request(subject, payload, 5*time.Second); err != nil {
				panic(err)
			}
		}
		fmt.Printf("ELAPSED_US %d\n", time.Since(start).Microseconds())
	case "sub":
		count, _ := strconv.Atoi(os.Args[4])
		ch := make(chan *nats.Msg, 65536)
		_, err := nc.ChanSubscribe(os.Args[3], ch)
		must(err)
		must(nc.Flush())
		fmt.Println("READY")
		var start time.Time
		for n := 0; n < count; n++ {
			m := <-ch
			if n == 0 {
				start = time.Now()
			}
			_ = m
		}
		fmt.Printf("RECV_US %d\n", time.Since(start).Microseconds())
	case "pub":
		count, _ := strconv.Atoi(os.Args[4])
		payload := []byte(strings.Repeat("x", must2(os.Args[5])))
		for i := 0; i < count; i++ {
			must(nc.Publish(os.Args[3], payload))
		}
		must(nc.Flush())
		fmt.Println("PUB_DONE")
	case "dial":
		count, _ := strconv.Atoi(os.Args[3])
		start := time.Now()
		for i := 0; i < count; i++ {
			c, err := nats.Connect(url, nats.NoReconnect())
			must(err)
			c.Close()
		}
		fmt.Printf("ELAPSED_US %d\n", time.Since(start).Microseconds())
	}
}

func must2(s string) int {
	n, err := strconv.Atoi(s)
	must(err)
	return n
}
