package main

import (
	"context"
	"fmt"
	"github.com/jxo-me/rabbitmq-go"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func bodyFrom(args []string) int {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "30"
	} else {
		s = strings.Join(args[1:], " ")
	}
	n, err := strconv.Atoi(s)
	failOnError(err, "Failed to convert arg to integer")
	return n
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	n := bodyFrom(os.Args)

	log.Printf(" [x] Requesting fib(%d)", n)
	ctx := context.Background()
	conn, err := rabbitmq.NewConn(
		ctx,
		"amqp://guest:guest@localhost",
		rabbitmq.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	client, err := rabbitmq.NewRpcClient(
		ctx,
		conn,
		rabbitmq.WithClientOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(ctx)

	//corrId := randomString(32)
	data, err := client.PublishWithContext(
		ctx,
		[]byte(strconv.Itoa(n)),
		"rpc_queue",
	)
	if err != nil {
		log.Println(err)
	}
	fmt.Println("rpc response data:", string(data))
}
