package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"time"
)

var addr = flag.String("bi", "stream.binance.com:9443", "binance webservice address")
var redisUrl = flag.String("redis", "127.0.0.1:6379", "Redis server address")
var redisPassword = flag.String("redis-pw", "", "Redis server password")
var redisDb = flag.Int("redis-db", 0, "Redis server password")
var channelNameFlag = flag.String("redis-channel", "BinanceAPI", "Redis server password")

func main() {
	flag.Parse()
	log.SetFlags(0)

	ctx := context.Background()
	channelName := *channelNameFlag

	rdb := redis.NewClient(&redis.Options{
		Addr:     *redisUrl,
		Password: *redisPassword, // no password set
		DB:       *redisDb,       // use default DB
	})

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	u := url.URL{Scheme: "wss", Host: *addr, Path: "/ws"}
	log.Printf("connecting to %s", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}
			log.Println("receve", message)
			rdb.Publish(ctx, channelName, message)
		}
		wg.Done()
	}()

	b := []byte(
		`{
  "method": "SUBSCRIBE",
  "params":["!ticker@arr"],
  "id":1
}`)
	var f interface{}
	err = json.Unmarshal(b, &f)
	if err != nil {
		log.Println(err)
	}
	err = c.WriteJSON(f)
	if err != nil {
		log.Println(err)
	}
	//wg.Wait()

	c1, cancel := context.WithCancel(context.Background())

	exitCh := make(chan struct{})
	go func(ctx context.Context) {
		for {

			select {
			case <-ctx.Done():
				fmt.Println("received done, exiting in 500 milliseconds")
				time.Sleep(500 * time.Millisecond)
				exitCh <- struct{}{}
				return
			default:
			}
		}
	}(c1)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	go func() {
		select {
		case <-signalCh:
			cancel()
			return
		}
	}()
	<-exitCh
}
