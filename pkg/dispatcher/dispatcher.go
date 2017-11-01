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

package dispatcher

import (
	"github.com/openzipkin/zipkin-go-opentracing"
	"net/http"
	"github.com/opentracing/opentracing-go"
)

type TraceContext interface {
	Close() error
	InitRequestSpan(r *http.Request) (FinishableSpan, error)
}

type FinishableSpan interface {
	Finish()
}

type HTTPTraceContext struct {
	Collector zipkintracer.Collector
	Tracer    opentracing.Tracer
	Span      opentracing.Span
}

type NoOpTraceContext struct{}
type NoOpSpan struct{}

type Dispatcher interface {
	Dispatch(in interface{}) (interface{}, error)
}

func (c *HTTPTraceContext) Close() error {
	return c.Collector.Close()
}

func (c *HTTPTraceContext) InitRequestSpan(r *http.Request) (FinishableSpan, error) {
	span := c.Tracer.StartSpan("http-span") //TODO: improve naming

	return span, c.Tracer.Inject(span.Context(), opentracing.TextMap, opentracing.HTTPHeadersCarrier(r.Header))
}

func (c *NoOpTraceContext) Close() error {
	return nil
}

func (c *NoOpTraceContext) InitRequestSpan(r *http.Request) (FinishableSpan, error) {
	return &NoOpSpan{}, nil
}

func (c *NoOpSpan) Finish() {}
