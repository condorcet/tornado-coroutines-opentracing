# coding: utf-8
from tornado import gen
from tornado.testing import AsyncTestCase
from opentracing.scope_managers.tornado import TornadoScopeManager
from opentracing.mocktracer import MockTracer
from opentracing import set_global_tracer, global_tracer


class _Base(AsyncTestCase):

    def setUp(self):
        super(_Base, self).setUp()
        set_global_tracer(MockTracer(TornadoScopeManager()))

    def wait_finished_spans(self, count, timeout=5.0):
        @gen.coroutine
        def wait():
            while len(global_tracer().finished_spans()) < count:
                yield gen.moment

        self.io_loop.run_sync(wait, timeout)
        finished_spans = global_tracer().finished_spans()
        assert len(finished_spans) == count
        return finished_spans


def is_parent_of(parent_span, *children):
    for child in children:
        assert parent_span.context.span_id == child.parent_id
    return True


def is_not_parent_of(parent_span, *children):
    for child in children:
        assert parent_span.context.span_id != child.parent_id
    return True


def has_no_parent(span):
    return span.parent_id is None


def empty_span(span, name):
    assert span.operation_name == name
    assert span.tags == {}
    assert len(span.logs) == 0
    return True


def has_exception(span, name, exc):
    assert span.operation_name == name
    assert span.tags == {'error': True}
    assert len(span.logs) == 1
    assert span.logs[0].key_values['error.object'] == exc
    return True
