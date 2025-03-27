from functools import wraps
from typing import Callable, Optional

from unstructured_ingest.logger import logger
from unstructured_ingest.otel import OtelHandler


def instrument(
    span_name: Optional[str] = None,
    record_exception: bool = True,
    attributes: dict[str, str] = None,
    log_out: Callable = logger.info,
) -> Callable[[Callable], Callable]:
    def span_decorator(func: Callable) -> Callable:
        def get_name(self) -> str:
            if span_name:
                return span_name
            return f"{self.identifier} step"

        @wraps(func)
        def wrap_with_span(self, *args, **kwargs):
            name = get_name(self=self)
            otel_handler = OtelHandler(otel_endpoint=self.context.otel_endpoint, log_out=log_out)
            with otel_handler.get_tracer().start_as_current_span(
                name, record_exception=record_exception
            ) as span:
                otel_handler.set_attributes(span, attributes)
                return func(self, *args, **kwargs)

        return wrap_with_span

    return span_decorator
