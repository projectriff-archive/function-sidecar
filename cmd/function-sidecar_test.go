package main_test

import (
	"testing"
	"net/http"
	"os/exec"
	"os"
	"fmt"
	"time"
	"gopkg.in/Shopify/sarama.v1"
	"bufio"
	"errors"
)

func TestIntegrationWithKafka(t *testing.T) {
	fmt.Println("TestIntegrationWithKafka invoked")

	SOURCE_MESSAGE_BODY := `{"key": "value"}`

	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		t.Fatal("Required environment variable KAFKA_BROKER was not provided")
	}

	fmt.Println("Building function-sidecar")

	buildCmd := exec.Command("go", "build", "-v", "function-sidecar.go")
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	buildCmd.Env = []string{"GOPATH=" + os.Getenv("GOPATH")}
	buildCmd.Run()

	cmd := exec.Command("./function-sidecar")

	configJson := fmt.Sprintf(`{
		"spring.cloud.stream.kafka.binder.brokers":"%s",
		"spring.cloud.stream.bindings.input.destination": "input-topic",
		"spring.cloud.stream.bindings.output.destination": "output-topic",
		"spring.cloud.stream.bindings.input.group": "test-group",
		"spring.profiles.active": "http"
	}`, broker)

	fmt.Println("Sidecar config: " + configJson)
	cmd.Env = []string{"SPRING_APPLICATION_JSON=" + configJson}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	fmt.Println("Starting function-sidecar")
	startErr := cmd.Start()

	if startErr != nil {
		t.Fatal(startErr)
	}
	fmt.Println("Waiting for function-sidecar to initalize")

	time.Sleep(5 * time.Second)

	messageReceived := false
	receivedMessageBody := ""

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		bodyScanner := bufio.NewScanner(r.Body)
		if ! bodyScanner.Scan() {
			t.Fatal(errors.New("Scan of message body failed"))

		}
		receivedMessageBody = bodyScanner.Text()
		fmt.Printf("HTTP call received: [%s]", receivedMessageBody)
		w.Write([]byte(receivedMessageBody))
		messageReceived = true
	})

	fmt.Println("Preparing to launch HTTP server")

	go func() {
		fmt.Println("Launching HTTP server")

		http.ListenAndServe(":8080", nil)
	}()

	fmt.Println(fmt.Sprintf("Sending test message to kafka at [%s]", broker))

	kafkaProducer, kafkaProducerErr := sarama.NewAsyncProducer([]string{broker}, nil)
	if kafkaProducerErr != nil {
		t.Fatal(kafkaProducerErr)
	}

	testMessage := &sarama.ProducerMessage{Topic: "input-topic", Value: sarama.StringEncoder(string([]byte{0xff, 0x00}) + SOURCE_MESSAGE_BODY)}
	kafkaProducer.Input() <- testMessage
	producerCloseErr := kafkaProducer.Close()
	if producerCloseErr != nil {
		t.Fatal(producerCloseErr)
	}

	fmt.Println("Message sent; waiting for HTTP call");
	for !messageReceived {
		fmt.Println("waiting...");
		time.Sleep(2 * time.Second)
	}

	if receivedMessageBody != SOURCE_MESSAGE_BODY {
		t.Fatal(fmt.Errorf("Received message [%s] does not match source mssage [%s]", receivedMessageBody, SOURCE_MESSAGE_BODY))
	}

	fmt.Println("TestIntegrationWithKafka ended");
}
