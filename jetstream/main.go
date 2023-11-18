package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var subject = "my_subject"

func main() {

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}

	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	InterestBasedConsumption(js)
	// QueueBasedConsumption(js)
	// PullConsumer(js)
	// KeyValue(js)
	ObjectStore(nc)
}

func printStreamState(ctx context.Context, stream jetstream.Stream) {
	info, _ := stream.Info(ctx)
	b, _ := json.MarshalIndent(info.State, "", " ")
	fmt.Println(string(b))
}

func InterestBasedConsumption(js jetstream.JetStream) {
	cfg := jetstream.StreamConfig{
		Name:      "ORDERS",
		Retention: jetstream.InterestPolicy,
		Subjects:  []string{"orders.>"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	stream, err := js.CreateStream(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Stream created:")

	js.Publish(ctx, "orders.page_loaded", nil)
	js.Publish(ctx, "orders.mouse_clicked", nil)
	ack, _ := js.Publish(ctx, "orders.input_focused", nil)
	fmt.Println("published 3 messages")
	if ack == nil {
		log.Fatal("no ack")
	}
	fmt.Printf("last message seq: %d\n", ack.Sequence)
	fmt.Println("# Stream info without any consumers")
	printStreamState(ctx, stream)

	// Adding a consumer
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "processor-1",
		AckPolicy: jetstream.AckExplicitPolicy,
	})

	js.Publish(ctx, "orders.mouse_clicked", nil)
	js.Publish(ctx, "orders.input_focused", nil)

	fmt.Println("\n# Stream info with one consumer")
	printStreamState(ctx, stream)

	msgs, _ := cons.Fetch(2)
	for msg := range msgs.Messages() {
		msg.DoubleAck(ctx)
	}

	fmt.Println("\n# Stream info with one consumer and acked messages")
	printStreamState(ctx, stream)

	cons2, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "processor-2",
		AckPolicy: jetstream.AckExplicitPolicy,
	})

	js.Publish(ctx, "orders.mouse_clicked", nil)
	js.Publish(ctx, "orders.input_focused", nil)

	msgs, _ = cons2.Fetch(2)
	var msgsMeta []*jetstream.MsgMetadata
	for msg := range msgs.Messages() {
		msg.DoubleAck(ctx)
		meta, _ := msg.Metadata()
		msgsMeta = append(msgsMeta, meta)
	}

	fmt.Printf("msg seqs %d and %d", msgsMeta[0].Sequence.Stream, msgsMeta[1].Sequence.Stream)

	fmt.Println("\n# Stream info with two consumers, but only one set of acked messages")
	printStreamState(ctx, stream)

	msgs, _ = cons.Fetch(2)
	for msg := range msgs.Messages() {
		msg.DoubleAck(ctx)
	}

	fmt.Println("\n# Stream info with two consumers having both acked")
	printStreamState(ctx, stream)

	stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "processor-3",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "orders.input_focused",
	})

	js.Publish(ctx, "orders.mouse_clicked", nil)

	msgs, _ = cons.Fetch(1)
	msg := <-msgs.Messages()
	msg.Term()
	msgs, _ = cons2.Fetch(1)
	msg = <-msgs.Messages()
	msg.DoubleAck(ctx)

	fmt.Println("\n# Stream info with three consumers with interest from two")
	printStreamState(ctx, stream)
}

func QueueBasedConsumption(js jetstream.JetStream) {
	cfg := jetstream.StreamConfig{
		Name:      "SALES",
		Retention: jetstream.WorkQueuePolicy,
		Subjects:  []string{"sales.>"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	stream, err := js.CreateStream(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Stream created:")

	js.Publish(ctx, "sales.us.page_loaded", nil)
	js.Publish(ctx, "sales.eu.mouse_clicked", nil)
	js.Publish(ctx, "sales.us.input_focused", nil)
	fmt.Println("published 3 messages")

	fmt.Println("# Stream info without any consumers")
	printStreamState(ctx, stream)

	cons, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name: "processor-1",
	})

	msgs, _ := cons.Fetch(3)
	for msg := range msgs.Messages() {
		msg.DoubleAck(ctx)
	}

	fmt.Println("\n# Stream info with one consumer")
	printStreamState(ctx, stream)

	_, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name: "processor-2",
	})
	fmt.Println("\n# Create an overlapping consumer")
	fmt.Println(err)

}

func PullConsumer(js jetstream.JetStream) {

	fmt.Println("PullConsumer")
	streamName := "CLICKS"
	cfg := jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"clicks.>"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	stream, err := js.CreateStream(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	js.Publish(ctx, "clicks.page_loaded.1", nil)
	js.Publish(ctx, "clicks.page_loaded.2", nil)
	js.Publish(ctx, "clicks.page_loaded.3", nil)

	// ephemeral consumer as Durable is not provided
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{})
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(3)

	cc, _ := cons.Consume(func(msg jetstream.Msg) {
		msg.Ack()
		fmt.Println("received msg on : ", msg.Subject())
		wg.Done()
	})

	wg.Wait()
	cc.Stop()

	js.Publish(ctx, "clicks.1", nil)
	js.Publish(ctx, "clicks.2", nil)
	js.Publish(ctx, "clicks.3", nil)

	msgs, _ := cons.Fetch(2)
	var i int
	for msg := range msgs.Messages() {

		msg.Ack()
		i++
	}
	fmt.Printf("got %d messages\n", i)

	msgs, err = cons.FetchNoWait(100)
	if err != nil {
		log.Fatal(err)
	}

	i = 0
	for msg := range msgs.Messages() {
		msg.Ack()
		i++
	}
	fmt.Printf("got %d messages\n", i)

	// At the end of the stream
	fetchStart := time.Now()
	msgs, _ = cons.Fetch(1, jetstream.FetchMaxWait(time.Second))
	i = 0

	for msg := range msgs.Messages() {
		msg.Ack()
		i++
	}
	fmt.Printf("got %d messages in %v\n", i, time.Since(fetchStart))

	dur, _ := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "processor",
	})

	msgs, _ = dur.Fetch(2)
	i = 0
	for msg := range msgs.Messages() {
		msg.Ack()
		fmt.Printf("received %q from durable consumer\n", msg.Subject())
		i++
	}
	fmt.Printf("got %d messages\n", i)

	stream.DeleteConsumer(ctx, "processor")

	_, err = stream.Consumer(ctx, "processor")

	fmt.Println("consumer deleted:", errors.Is(err, jetstream.ErrConsumerNotFound))
}

func KeyValue(js jetstream.JetStream) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	kv, _ := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "profiles",
	})

	kv.Put(ctx, "sue.color", []byte("blue"))
	entry, _ := kv.Get(ctx, "sue.color")
	fmt.Printf("%s @ %d -> %q\n", entry.Key(), entry.Revision(), string(entry.Value()))

	kv.Put(ctx, "sue.color", []byte("green"))
	entry, _ = kv.Get(ctx, "sue.color")
	fmt.Printf("%s @ %d -> %q\n", entry.Key(), entry.Revision(), string(entry.Value()))

	_, err := kv.Update(ctx, "sue.color", []byte("red"), 1)
	fmt.Printf("expected error: %s\n", err)

	kv.Update(ctx, "sue.color", []byte("red"), 2)
	entry, _ = kv.Get(ctx, "sue.color")
	fmt.Printf("%s @ %d -> %q\n", entry.Key(), entry.Revision(), string(entry.Value()))

	names := js.StreamNames(ctx)
	name := ""
	for n := range names.Name() {
		log.Println(n)
		if strings.HasPrefix(n, "KV") {
			name = n
		}
	}

	fmt.Printf("KV stream name: %s\n", name)

	cons, _ := js.CreateOrUpdateConsumer(ctx, name, jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckNonePolicy,
	})

	msg, _ := cons.Next()
	md, _ := msg.Metadata()
	fmt.Printf("%s @ %d -> %q\n", msg.Subject(), md.Sequence.Stream, string(msg.Data()))

	kv.Put(ctx, "sue.color", []byte("yellow"))
	msg, _ = cons.Next()
	md, _ = msg.Metadata()
	fmt.Printf("%s @ %d -> %q\n", msg.Subject(), md.Sequence.Stream, string(msg.Data()))

	kv.Delete(ctx, "sue.color")
	msg, _ = cons.Next()
	md, _ = msg.Metadata()
	fmt.Printf("%s @ %d -> %q\n", msg.Subject(), md.Sequence.Stream, msg.Data())

	fmt.Printf("headers: %v\n", msg.Headers())

	w, _ := kv.Watch(ctx, "sue.*")
	defer w.Stop()

	kve := <-w.Updates()
	fmt.Printf("%s @ %d -> %q (op: %s)\n", kve.Key(), kve.Revision(), string(kve.Value()), kve.Operation())

	kv.Put(ctx, "sue.color", []byte("purple"))
	<-w.Updates()

	kve = <-w.Updates()
	fmt.Printf("%s @ %d -> %q (op: %s)\n", kve.Key(), kve.Revision(), string(kve.Value()), kve.Operation())

	kv.Put(ctx, "sue.food", []byte("pizza"))

	kve = <-w.Updates()
	fmt.Printf("%s @ %d -> %q (op: %s)\n", kve.Key(), kve.Revision(), string(kve.Value()), kve.Operation())
}

func ObjectStore(nc *nats.Conn) {

	jsm, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	objectStoreName := "test_store"
	obj, err := jsm.CreateObjectStore(&nats.ObjectStoreConfig{
		Bucket:      objectStoreName,
		Description: "Test object store",
	})
	if err != nil {
		log.Fatal(err)
	}
	obj_status, _ := obj.Status()
	log.Println(obj_status.Size())

	// js.
	bytesLen := 10000000
	data := []byte(strings.Repeat("a", bytesLen))
	objName := "data blob"
	putObjInfo, err := obj.Put(&nats.ObjectMeta{
		Name:        objName,
		Description: "large blob of data",
	}, bytes.NewReader(data))

	if err != nil {
		log.Fatal(err)
	}

	log.Println(putObjInfo.Name, putObjInfo.Size, putObjInfo.Description)

	obj.UpdateMeta(objName, &nats.ObjectMeta{
		Description: " still large blob of data",
	})

	listOfObj, _ := obj.List()
	for _, obj := range listOfObj {
		fmt.Println(obj.Name)
		fmt.Println(obj.Size)
	}

	retrieveData, _ := obj.Get(objName)
	retrieveDataInfo, _ := retrieveData.Info()
	fmt.Println("retrieveDataInfosize : ", retrieveDataInfo.Size)
	fmt.Println("nc client maxpayload : ", nc.MaxPayload())

	// create a watcher
	watcher, _ := obj.Watch()
	defer watcher.Stop()

	res := <-watcher.Updates()
	if res != nil {
		fmt.Println(res.Name)
		fmt.Println(res.Chunks)
		fmt.Println(res.Size)
	}

	res = <-watcher.Updates()
	if res != nil {
		fmt.Println(res.Name)
		fmt.Println(res.Chunks)
		fmt.Println(res.Size)
	}

	retrieveData, _ = obj.Get("orders.json")

	buf := make([]byte, 200) // Adjust size as needed
	for {
		n, err := retrieveData.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		fmt.Println(string(buf[:n]))
	}
	jsm.DeleteObjectStore(objectStoreName)
}
