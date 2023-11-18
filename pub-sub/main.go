package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url)
	if err != nil {
		log.Fatal(err)
	}

	defer nc.Drain()

	nc.Publish("greet.john", []byte("hello"))

	// sync subscriber
	sub, _ := nc.SubscribeSync("greet.*")

	// will be empty as publish is before
	msg, _ := sub.NextMsg(10 * time.Millisecond)
	fmt.Println("Subscribed after publish...")
	fmt.Printf("msg is nil ? %v\n", msg == nil)

	nc.Publish("greet.joe", []byte("hello"))
	nc.Publish("greet.pam", []byte("hello"))

	// since subscription is set, we can now receive the message
	msg, _ = sub.NextMsg(10 * time.Millisecond)
	fmt.Printf("msg data : %q on subject %q\n", string(msg.Data), msg.Subject)

	msg, _ = sub.NextMsg(10 * time.Millisecond)
	fmt.Printf("msg data : %q on subject %q\n", string(msg.Data), msg.Subject)

	nc.Publish("greet.bob", []byte("hello"))
	msg, _ = sub.NextMsg(10 * time.Millisecond)
	fmt.Printf("msg data : %q on subject %q\n", string(msg.Data), msg.Subject)
}
