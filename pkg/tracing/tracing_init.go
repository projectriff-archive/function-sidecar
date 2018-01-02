package tracing

import (
	"log"

	"github.com/opentracing/opentracing-go"
	"github.com/openzipkin/zipkin-go-opentracing"
)

func BuildTraceContext(zipkinUrl string, zipkinServiceName string) (TraceContext, error) {
	if zipkinUrl == "" {
		return &NoOpTraceContext{}, nil
	}

	if zipkinServiceName == "" {
		zipkinServiceName = "default-service"
	}

	log.Printf("Init tracing against [%s] for service [%s]", zipkinUrl, zipkinServiceName)

	return buildHTTPTraceContext(zipkinUrl, zipkinServiceName)
}

func buildHTTPTraceContext(zipkinUrl string, zipkinServiceName string) (TraceContext, error) {
	zipkinCollector, zipkinInitErr := zipkintracer.NewHTTPCollector(zipkinUrl + "/api/v1/spans")
	if zipkinInitErr != nil {
		panic(zipkinInitErr)
	}

	zipkinRecorder := zipkintracer.NewRecorder(zipkinCollector, true, "0.0.0.0:0", zipkinServiceName)
	zipkinTracer, tracerErr := zipkintracer.NewTracer(zipkinRecorder, zipkintracer.ClientServerSameSpan(true), zipkintracer.TraceID128Bit(true))
	if tracerErr != nil {
		panic(tracerErr)
	}

	opentracing.InitGlobalTracer(zipkinTracer)
	return &HTTPTraceContext{Collector: zipkinCollector, Tracer: zipkinTracer}, nil
}
