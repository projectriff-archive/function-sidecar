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

package http

import (
	"github.com/sk8sio/function-sidecar/pkg/dispatcher"
	"net/http"
	"bytes"
	"io/ioutil"
	"log"
	"time"
)

type httpDispatcher struct {
	traceContext dispatcher.TraceContext
}

func (d *httpDispatcher) Dispatch(in interface{}) (interface{}, error) {
	slice := ([]byte)(in.(string))

	requestToInvoker, requestErr := http.NewRequest("POST", "http://localhost:8080", bytes.NewReader(slice))
	if requestErr != nil {
		return nil, requestErr
	}
	requestToInvoker.Header["Content-Type"] = []string{"text/plain"}

	span, spanErr := d.traceContext.InitRequestSpan(requestToInvoker)

	if spanErr != nil {
		return nil, spanErr
	}
	defer span.Finish()

	client := http.Client{
		Timeout: time.Duration(60 * time.Second),
	}
	resp, err := client.Do(requestToInvoker)

	if err != nil {
		log.Printf("Error invoking http://localhost:8080: %v", err)
		return nil, err
	}
	defer resp.Body.Close()
	out, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response", err)
		return nil, err
	}

	return string(out), nil
}

func NewHttpDispatcher(traceContext dispatcher.TraceContext) dispatcher.Dispatcher {
	return &httpDispatcher{traceContext: traceContext}
}
