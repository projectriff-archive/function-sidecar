/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bsm/sarama-cluster"
	"gopkg.in/Shopify/sarama.v1"

	"flag"
	"github.com/sk8sio/function-sidecar/pkg/dispatcher"
	"github.com/sk8sio/function-sidecar/pkg/dispatcher/grpc"
	"github.com/sk8sio/function-sidecar/pkg/dispatcher/http"
	"github.com/sk8sio/function-sidecar/pkg/dispatcher/stdio"
	"github.com/sk8sio/function-sidecar/pkg/message"
	"strings"
)

type stringSlice []string

func (sl *stringSlice) String() string {
	return fmt.Sprint(*sl)
}

func (sl *stringSlice) Set(value string) error {
	*sl = stringSlice(strings.Split(value, ","))
	return nil
}

//var brokers stringSlice = []string{"localhost:9092"} // TODO uncomment after switch
var brokers, inputs, outputs stringSlice
var group, protocol string

func init() {
	flag.Var(&brokers, "brokers", "location of the Kafka server(s) to connect to")
	flag.Var(&inputs, "inputs", "kafka topic(s) to listen to, as input for the function")
	flag.Var(&outputs, "outputs", "kafka topic(s) to write to with results from the function")
	flag.StringVar(&group, "group", "", "kafka consumer group to act as")
	flag.StringVar(&protocol, "protocol", "", "dispatcher protocol to use. One of [http, grpc, stdio]")
}

func main() {

	flag.Parse()

	if group == "" { // TODO, drop after switch
		var saj map[string]interface{}
		err := json.Unmarshal([]byte(os.Getenv("SPRING_APPLICATION_JSON")), &saj)
		if err != nil {
			panic(err)
		}
		brokers = []string{saj["spring.cloud.stream.kafka.binder.brokers"].(string)}
		inputs = []string{saj["spring.cloud.stream.bindings.input.destination"].(string)}
		jOutput := saj["spring.cloud.stream.bindings.output.destination"]
		if jOutput != nil {
			outputs = []string{jOutput.(string)}
		}
		group = saj["spring.cloud.stream.bindings.input.group"].(string)
		protocol = saj["spring.profiles.active"].(string)
	}

	input := inputs[0]
	var output string
	if len(outputs) > 0 {
		output = outputs[0]
	} else {
		output = ""
	}

	fmt.Printf("Sidecar for function '%v' (%v->%v) using %v dispatcher starting\n", group, input, output, protocol)

	var producer sarama.AsyncProducer
	var err error
	if output != "" {
		producer, err = sarama.NewAsyncProducer(brokers, nil)
		if err != nil {
			panic(err)
		}
		defer producer.Close()
	}

	consumerConfig := makeConsumerConfig()
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := cluster.NewConsumer(brokers, group, []string{input}, consumerConfig)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	if consumerConfig.Consumer.Return.Errors {
		go consumeErrors(consumer)
	}
	if consumerConfig.Group.Return.Notifications {
		go consumeNotifications(consumer)
	}

	dispatcher := createDispatcher(protocol)

	// trap SIGINT, SIGTERM, and SIGKILL to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, os.Kill)

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				messageIn, err := message.ExtractMessage(msg.Value)
				fmt.Fprintf(os.Stdout, "<<< %s\n", messageIn)
				if err != nil {
					log.Printf("Error receiving message from Kafka: %v", err)
					break
				}
				strPayload := string(messageIn.Payload.([]byte))
				dispatched, err := dispatcher.Dispatch(strPayload)
				if err != nil {
					log.Printf("Error dispatching message: %v", err)
					break
				}
				if output != "" {
					messageOut := message.Message{Payload: []byte(dispatched.(string)), Headers: messageIn.Headers}
					bytesOut, err := message.EncodeMessage(messageOut)
					fmt.Fprintf(os.Stdout, ">>> %s\n", messageOut)
					if err != nil {
						log.Printf("Error encoding message: %v", err)
						break
					}
					outMessage := &sarama.ProducerMessage{Topic: output, Value: sarama.ByteEncoder(bytesOut)}
					producer.Input() <- outMessage
				} else {
					fmt.Fprintf(os.Stdout, "=== Not sending function return value as function did not provide an output channel. Raw result = %s\n", dispatched)
				}
				consumer.MarkOffset(msg, "") // mark message as processed
			}
		case <-signals:
			return
		}
	}
}

func createDispatcher(protocol string) dispatcher.Dispatcher {
	switch protocol {
	case "http":
		return http.NewHttpDispatcher()
	case "stdio":
		return stdio.NewStdioDispatcher()
	case "grpc":
		return grpc.NewGrpcDispatcher()
	default:
		panic("Unsupported Dispatcher " + protocol)
	}
}

func consumeNotifications(consumer *cluster.Consumer) {
	for ntf := range consumer.Notifications() {
		log.Printf("Rebalanced: %+v\n", ntf)
	}
}

func consumeErrors(consumer *cluster.Consumer) {
	for err := range consumer.Errors() {
		log.Printf("Error: %s\n", err.Error())
	}
}

func makeConsumerConfig() *cluster.Config {
	consumerConfig := cluster.NewConfig()
	consumerConfig.Consumer.Return.Errors = true
	consumerConfig.Group.Return.Notifications = true
	return consumerConfig
}
