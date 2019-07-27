# coding: utf-8
import pytest
from opentracing import global_tracer
from tornado import gen
from tornado_coroutines_opentracing import ff_coroutine
from tornado.testing import gen_test

from . import _Base, empty_span, has_exception, is_parent_of


class YieldingTestCase(_Base):
    """
    Yielding decorated coroutine works as well as original `gen.coroutine`.

    Exception in such coroutine propagates to their parent span and logged, if
    such the parent span exists (see test_yield_exception_with_own_span below).

    It is expected behaviour must not be broken by the decorator.
    """

    @ff_coroutine
    def coro(self, value=None, exc=None):
        yield gen.sleep(0.1)
        if exc:
            raise exc
        if value is not None:
            raise gen.Return(value)

    @ff_coroutine
    def coro_with_span(self, name, value=None, exc=None):
        with global_tracer().start_active_span(
                operation_name=name,
                child_of=global_tracer().active_span
        ):
            yield gen.sleep(0.1)
            if exc:
                raise exc
        if value is not None:
            raise gen.Return(value)

    @gen_test
    def test_yield_without_any_spans(self):

        res = yield self.coro(value=42)

        assert res == 42
        assert len(global_tracer().finished_spans()) == 0

    @gen_test
    def test_yield_without_root_span(self):

        res = yield self.coro_with_span('coro', 42)

        assert res == 42

        coro, = global_tracer().finished_spans()
        assert empty_span(coro, 'coro')

    @gen_test
    def test_yield_coro_without_span(self):

        with global_tracer().start_active_span('root'):
            result = yield self.coro(value='foobar')

        assert result == 'foobar'

        root, = global_tracer().finished_spans()
        assert empty_span(root, 'root')

    @gen_test
    def test_yield_with_span(self):

        with global_tracer().start_active_span('root'):
            result = yield self.coro_with_span('coro', value='foobar')

        assert result == 'foobar'

        coro, root = global_tracer().finished_spans()
        assert empty_span(root, 'root')

    @gen_test
    def test_yield_exception(self):

        exc = Exception('foobar')

        with pytest.raises(Exception, match='foobar'):
            with global_tracer().start_active_span('root'):
                yield self.coro(exc=exc)

        root, = global_tracer().finished_spans()

        assert has_exception(root, 'root', exc)

    @gen_test
    def test_yield_exception_with_own_span(self):

        exc = Exception('foobar')

        with pytest.raises(Exception, match='foobar'):
            with global_tracer().start_active_span('root'):
                yield self.coro_with_span('coro', exc=exc)

        coro, root, = global_tracer().finished_spans()

        assert has_exception(coro, 'coro', exc)
        assert has_exception(root, 'root', exc)

        assert is_parent_of(root, coro)
