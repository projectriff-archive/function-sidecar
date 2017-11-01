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
	const EXPECTED_MESSAGE_COUNT = 5
	receivedMessageCount := 0

	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		t.Fatal("Required environment variable KAFKA_BROKER was not provided")
	}

	fmt.Println("Building function-sidecar")

	buildCmd := exec.Command("go", "build", "-v", "function-sidecar.go")
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	buildCmd.Env = []string{"GOPATH=" + os.Getenv("GOPATH"), "PATH=" + os.Getenv("PATH")}
	buildErr := buildCmd.Run()
	if buildErr != nil {
		t.Fatal(buildErr)
	}

	cmd := exec.Command("./function-sidecar")

	inputTopic := fmt.Sprintf("input-%d", time.Now().UnixNano())

	configJson := fmt.Sprintf(`{
		"spring.cloud.stream.kafka.binder.brokers":"%s",
		"spring.cloud.stream.bindings.input.destination": "%s",
		"spring.cloud.stream.bindings.output.destination": "output-%d",
		"spring.cloud.stream.bindings.input.group": "test-group",
		"spring.profiles.active": "http"
	}`, broker, inputTopic, time.Now().UnixNano())

	fmt.Println("Sidecar config: " + configJson)
	cmd.Env = []string{"SPRING_APPLICATION_JSON=" + configJson, "SIDECAR_CONFIG=" + os.Getenv("SIDECAR_CONFIG")}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	fmt.Println("Starting function-sidecar")
	startErr := cmd.Start()

	if startErr != nil {
		t.Fatal(startErr)
	}
	fmt.Println("Waiting for function-sidecar to initalize")

	time.Sleep(1 * time.Second)

	receivedMessageBody := ""

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		bodyScanner := bufio.NewScanner(r.Body)
		if ! bodyScanner.Scan() {
			t.Fatal(errors.New("Scan of message body failed"))

		}
		receivedMessageBody = bodyScanner.Text()
		fmt.Printf("HTTP call received: [%s]", receivedMessageBody)
		w.Write([]byte(receivedMessageBody))
		receivedMessageCount++
	})

	fmt.Println("Preparing to launch HTTP server")

	go func() {
		fmt.Println("Launching HTTP server")

		http.ListenAndServe(":8080", nil)
	}()

	fmt.Println(fmt.Sprintf("Sending test message to kafka at [%s]", broker))

	fmt.Println("Message sent; waiting for HTTP call");
	for receivedMessageCount < EXPECTED_MESSAGE_COUNT {
		fmt.Println("waiting...");
		time.Sleep(1 * time.Second)

		kafkaProducer, kafkaProducerErr := sarama.NewAsyncProducer([]string{broker}, nil)
		if kafkaProducerErr != nil {
			t.Fatal(kafkaProducerErr)
		}

		kafkaProducer.Input() <- &sarama.ProducerMessage{Topic: inputTopic, Value: sarama.StringEncoder(string([]byte{0xff, 0x00}) + SOURCE_MESSAGE_BODY)}
		producerCloseErr := kafkaProducer.Close()
		if producerCloseErr != nil {
			t.Fatal(producerCloseErr)
		}
	}

	if receivedMessageBody != SOURCE_MESSAGE_BODY {
		t.Fatal(fmt.Errorf("Received message [%s] does not match source mssage [%s]", receivedMessageBody, SOURCE_MESSAGE_BODY))
	}
	cmd.Process.Signal(os.Kill)

	fmt.Println("TestIntegrationWithKafka ended");
}
