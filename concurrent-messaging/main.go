package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/nats-io/nats.go"
)

type Payload struct {
	Foo string `json:"foo"`
	Bar int    `json:"bar"`
}

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
		var p Payload
		if err := json.Unmarshal(msg.Data, &p); err != nil {
			fmt.Printf("received your JSON payload : %s\n", msg.Data)
		} else {
			fmt.Printf("received valid JSON payload: Foo=%s Bar=%d\n", p.Foo, p.Bar)
		}
	})

	if err != nil {
		log.Fatal(err)
	}

	// Construct a Payload value and serialize it.
	payload := Payload{Foo: "bar", Bar: 27}
	bytes, err := json.Marshal(payload)
	if err != nil {
		log.Fatal(err)
	}

	nc.Publish("greet.tata", bytes)
	nc.Publish("greet.toto", []byte("not a json"))

	<-wait
}
