import asyncio
import logging
import multiprocessing as mp
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, TypeVar

from tqdm import tqdm
from tqdm.asyncio import tqdm as tqdm_asyncio

from unstructured_ingest.v2.interfaces import BaseProcess, ProcessorConfig, Uploader
from unstructured_ingest.v2.logger import logger, make_default_logger
from unstructured_ingest.v2.otel import OtelHandler
from unstructured_ingest.v2.pipeline.otel import instrument

BaseProcessT = TypeVar("BaseProcessT", bound=BaseProcess)
iterable_input = list[dict[str, Any]]


@dataclass
class PipelineStep(ABC):
    process: BaseProcessT
    context: ProcessorConfig
    identifier: str

    def __str__(self):
        return self.identifier

    def process_serially(self, iterable: iterable_input) -> Any:
        logger.info("processing content serially")
        if iterable:
            if len(iterable) == 1:
                return [self.run(**iterable[0])]
            if self.context.tqdm:
                return [self.run(**it) for it in tqdm(iterable, desc=self.identifier)]
            return [self.run(**it) for it in iterable]
        return [self.run()]

    async def _process_async(self, iterable: iterable_input) -> Any:
        if iterable:
            if len(iterable) == 1:
                return [await self.run_async(**iterable[0])]
            if self.context.tqdm:
                return await tqdm_asyncio.gather(
                    *[self.run_async(**i) for i in iterable], desc=self.identifier
                )
            return await asyncio.gather(*[self.run_async(**i) for i in iterable])
        return [await self.run_async()]

    def process_async(self, iterable: iterable_input) -> Any:
        logger.info("processing content async")
        return self.asyncio_run(fn=self._process_async, iterable=iterable)

    def asyncio_run(
        self, fn: Callable[[Any, Any], Awaitable[Any]], *args: Any, **kwargs: Any
    ) -> Any:
        current_loop = asyncio._get_running_loop()
        if current_loop is None:
            return asyncio.run(fn(*args, **kwargs))
        with ThreadPoolExecutor(thread_name_prefix="asyncio") as thread_pool:
            logger.warning(
                f"async code being run in dedicated thread pool "
                f"to not conflict with existing event loop: {current_loop}"
            )

            def wrapped():
                return asyncio.run(fn(*args, **kwargs))

            future = thread_pool.submit(wrapped)
            return future.result()

    def process_multiprocess(self, iterable: iterable_input) -> Any:
        logger.info("processing content across processes")

        if iterable:
            if len(iterable) == 1:
                return self.process_serially(iterable)
            if self.context.num_processes == 1:
                return self.process_serially(iterable)
            with mp.Pool(
                processes=self.context.num_processes,
                initializer=self._init_mp,
                initargs=(
                    logging.DEBUG if self.context.verbose else logging.INFO,
                    self.context.otel_endpoint,
                ),
            ) as pool:
                otel_context = OtelHandler.inject_context()
                for iter in iterable:
                    iter[OtelHandler.trace_context_key] = otel_context
                if self.context.tqdm:
                    return list(
                        tqdm(
                            pool.imap_unordered(func=self._wrap_mp, iterable=iterable),
                            total=len(iterable),
                            desc=self.identifier,
                        )
                    )
                return pool.map(self._wrap_mp, iterable)
        return [self.run()]

    def _wrap_mp(self, input_kwargs: dict) -> Any:
        # Allow mapping of kwargs via multiprocessing map()
        return self.run(**input_kwargs)

    def _init_mp(self, log_level: int, endpoint: Optional[str] = None) -> None:
        # Init logger for each spawned process when using multiprocessing pool
        make_default_logger(level=log_level)
        otel_handler = OtelHandler(otel_endpoint=endpoint, log_out=logger.debug)
        otel_handler.init_trace()

    @instrument()
    def __call__(self, iterable: Optional[iterable_input] = None) -> Any:
        iterable = iterable or []
        if iterable:
            logger.info(
                f"Calling {self.__class__.__name__} " f"with {len(iterable)} docs",  # type: ignore
            )
        else:
            logger.info(f"Calling {self.__class__.__name__} with no inputs")
        if self.context.async_supported and self.process.is_async():
            return self.process_async(iterable=iterable)
        if self.context.mp_supported:
            return self.process_multiprocess(iterable=iterable)
        return self.process_serially(iterable=iterable)

    def _run(self, fn: Callable, **kwargs: Any) -> Optional[Any]:
        return self.asyncio_run(fn=self.run_async, _fn=fn, **kwargs)

    async def _run_async(self, fn: Callable, **kwargs: Any) -> Optional[Any]:
        raise NotImplementedError

    def run(self, _fn: Optional[Callable] = None, **kwargs: Any) -> Optional[Any]:
        kwargs = kwargs.copy()
        otel_handler = OtelHandler(otel_endpoint=self.context.otel_endpoint, log_out=logger.debug)
        tracer = otel_handler.get_tracer()
        if trace_context := kwargs.pop(otel_handler.trace_context_key, {}):
            otel_handler.attach_context(trace_context=trace_context)
        attributes = {}
        if file_data_path := kwargs.get("file_data_path"):
            attributes["file_id"] = Path(file_data_path).stem
        try:
            with tracer.start_as_current_span(self.identifier, record_exception=True) as span:
                otel_handler.set_attributes(span, attributes)
                fn = _fn or self.process.run
                return self._run(fn=fn, **kwargs)
        except Exception as e:
            logger.error(f"Exception raised while running {self.identifier}", exc_info=e)
            if "file_data_path" in kwargs:
                self.context.status[kwargs["file_data_path"]] = {self.identifier: str(e)}
            if self.context.raise_on_error:
                raise e
            return None

    async def run_async(self, _fn: Optional[Callable] = None, **kwargs: Any) -> Optional[Any]:
        otel_handler = OtelHandler(otel_endpoint=self.context.otel_endpoint, log_out=logger.debug)
        try:
            attributes = {}
            if file_data_path := kwargs.get("file_data_path"):
                attributes["file_id"] = Path(file_data_path).stem
            with otel_handler.get_tracer().start_as_current_span(
                self.identifier, record_exception=True
            ) as span:
                otel_handler.set_attributes(span, attributes)
                fn = _fn or self.process.run_async
                return await self._run_async(fn=fn, **kwargs)
        except Exception as e:
            logger.error(f"Exception raised while running {self.identifier}", exc_info=e)
            if "file_data_path" in kwargs:
                self.context.status[kwargs["file_data_path"]] = {self.identifier: str(e)}
            if self.context.raise_on_error:
                raise e
            return None

    @property
    def cache_dir(self) -> Path:
        return Path(self.context.work_dir) / self.identifier


@dataclass
class BatchPipelineStep(PipelineStep, ABC):
    process: Uploader

    def __call__(self, iterable: Optional[iterable_input] = None) -> Any:
        if self.context.mp_supported and self.process.is_batch():
            return self.run_batch(contents=iterable)
        super().__call__(iterable=iterable)

    @abstractmethod
    def _run_batch(self, contents: iterable_input, **kwargs) -> Any:
        pass

    def run_batch(self, contents: iterable_input, **kwargs) -> Any:
        try:
            return self._run_batch(contents=contents, **kwargs)
        except Exception as e:
            self.context.status[self.identifier] = {"step_error": str(e)}
            if self.context.raise_on_error:
                raise e
            return None
