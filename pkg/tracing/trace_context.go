package tracing

import (
	"net/http"

	"github.com/opentracing/opentracing-go"
	"github.com/openzipkin/zipkin-go-opentracing"
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
