import os
from dataclasses import dataclass, field
from typing import Callable, Optional, Protocol, Sequence

from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import ReadableSpan, Tracer, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    SpanExportResult,
)

from unstructured_ingest.v2.logger import logger


class AddTraceCallable(Protocol):
    def __call__(self, provider: TracerProvider) -> None:
        pass


class LogSpanExporter(ConsoleSpanExporter):
    def __init__(self, log_out: Callable = logger.info, **kwargs):
        self.log_out = log_out
        super().__init__(**kwargs)

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        for span in spans:
            self.log_out(self.formatter(span))
        return SpanExportResult.SUCCESS


@dataclass
class OtelHandler:
    otel_endpoint: Optional[str] = None
    service_name: str = "unstructured-ingest"
    trace_provider: TracerProvider = field(init=False)
    log_out: Callable = field(default=logger.info)

    def __post_init__(self):
        resource = Resource(attributes={SERVICE_NAME: self.service_name})
        self.trace_provider = self.init_trace_provider(resource=resource)

        trace._set_tracer_provider(self.trace_provider, log=False)

    def get_otel_endpoint(self) -> Optional[str]:
        if otel_endpoint := self.otel_endpoint:
            return otel_endpoint
        if otlp_endpoint := os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
            return otlp_endpoint
        if otlp_traces_endpoint := os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"):
            return otlp_traces_endpoint
        return None

    def _add_console_trace_processor(self, provider: TracerProvider) -> None:
        def custom_formatter(span: ReadableSpan) -> str:
            duration = (span.end_time - span.start_time) / 1e9
            return f"{span.name} finished in {duration}s\n"

        tracer_exporter = LogSpanExporter(formatter=custom_formatter, log_out=self.log_out)
        processor = SimpleSpanProcessor(tracer_exporter)
        provider.add_span_processor(span_processor=processor)

    def _add_otel_trace_processor(self, provider: TracerProvider) -> None:
        otel_endpoint = self.get_otel_endpoint()
        if not otel_endpoint:
            return None
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        trace_exporter = OTLPSpanExporter()
        processor = BatchSpanProcessor(trace_exporter)
        provider.add_span_processor(processor)

    def init_trace_provider(self, resource: Resource) -> TracerProvider:
        trace_provider = TracerProvider(resource=resource)
        add_fns: list[AddTraceCallable] = [
            self._add_otel_trace_processor,
            self._add_console_trace_processor,
        ]
        for add_fn in add_fns:
            add_fn(provider=trace_provider)
        return trace_provider

    def get_tracer(self) -> Tracer:
        return trace.get_tracer(self.service_name)
