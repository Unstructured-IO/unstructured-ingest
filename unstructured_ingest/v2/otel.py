import os
from dataclasses import dataclass, field
from typing import Callable, ClassVar, Optional, Protocol, Sequence

from opentelemetry import trace
from opentelemetry.context import attach, get_current
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import ReadableSpan, Tracer, TracerProvider
from opentelemetry.sdk.trace.export import (
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
    trace_context_key: ClassVar[str] = "_trace_context"

    def init_trace(self):
        # Should only be done once
        resource = Resource(attributes={SERVICE_NAME: self.service_name})
        trace_provider = self.init_trace_provider(resource=resource)
        trace.set_tracer_provider(trace_provider)

    @staticmethod
    def set_attributes(span, attributes_dict):
        if attributes_dict:
            for att in attributes_dict:
                span.set_attribute(att, attributes_dict[att])

    @staticmethod
    def inject_context() -> dict:
        trace_context = {}
        current_context = get_current()
        inject(trace_context, current_context)
        return trace_context

    @staticmethod
    def attach_context(trace_context: dict) -> object:
        extracted_context = extract(trace_context)
        return attach(extracted_context)

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
            s = f"{span.name} finished in {duration}s"
            if span.attributes:
                attributes_str = ", ".join([f"{k}={v}" for k, v in span.attributes.items()])
                s += f", attributes: {attributes_str}"
            return s

        tracer_exporter = LogSpanExporter(formatter=custom_formatter, log_out=self.log_out)
        processor = SimpleSpanProcessor(tracer_exporter)
        provider.add_span_processor(span_processor=processor)

    def _add_otel_trace_processor(self, provider: TracerProvider) -> None:
        otel_endpoint = self.get_otel_endpoint()
        if not otel_endpoint:
            return None
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        logger.debug(f"Adding otel exported at {otel_endpoint}")
        trace_exporter = OTLPSpanExporter()
        processor = SimpleSpanProcessor(trace_exporter)
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
