from functools import wraps
from typing import Callable, Optional

from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.otel import OtelHandler


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
            if func.__name__ == "__call__":
                return f"{self.__class__.__name__} [cls]"
            else:
                return func.__name__

        def _set_attributes(span, attributes_dict):
            if attributes_dict:
                for att in attributes_dict:
                    span.set_attribute(att, attributes_dict[att])

        @wraps(func)
        def wrap_with_span(self, *args, **kwargs):
            name = get_name(self=self)
            otel_handler = OtelHandler(otel_endpoint=self.context.otel_endpoint, log_out=log_out)
            with otel_handler.get_tracer().start_as_current_span(
                name, record_exception=record_exception
            ) as span:
                _set_attributes(span, attributes)
                return func(self, *args, **kwargs)

        return wrap_with_span

    return span_decorator
