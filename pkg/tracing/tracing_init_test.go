package tracing_test

import (
	"testing"

	"github.com/projectriff/function-sidecar/pkg/tracing"
)

func TestTracing(t *testing.T) {
	traceContext, err := tracing.BuildTraceContext("someserver:9411", "")
	if err != nil {
		t.Fatalf("Failed to build test trace context: [%v]", err)
	}

	httpTraceContext := traceContext.(*tracing.HTTPTraceContext)

	if httpTraceContext.Tracer == nil {
		t.Fatalf("Trace context incorrectly initialized; missing tracer reference")
	}

	if httpTraceContext.Collector == nil {
		t.Fatalf("Trace context incorrectly initialized; missing collector reference")
	}
}
