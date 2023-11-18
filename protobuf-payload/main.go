package main

import (
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/learn-nats.com/protobuf-payload"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

func main() {

	wait := make(chan bool)

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url)
	if err != nil {
		log.Fatal(err)
	}

	defer nc.Drain()
	_, err = nc.Subscribe("greet.*", func(msg *nats.Msg) {
		var req GreetRequest
		proto.Unmarshal(msg.Data, &req)

		rep := GreetReply{
			Text: fmt.Sprintf("hello %q\n", req.Name),
		}

		data, _ := proto.Marshal(&rep)
		msg.Respond(data)
	})

	if err != nil {
		log.Fatal(err)
	}

	// Construct a payload value and serialize it.
	req := GreetRequest{
		Name: "Joe",
	}
	data, _ := proto.Marshal(&req)
	msg, _ := nc.Request("greet", data, 10*time.Millisecond)

	var rep GreetReply
	proto.Unmarshal(msg.Data, &rep)
	fmt.Printf("reply : %s\n", rep.Text)

	<-wait
}
