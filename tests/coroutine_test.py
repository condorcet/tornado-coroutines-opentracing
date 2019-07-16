# coding: utf-8
import tornado_coroutines_opentracing

from opentracing import global_tracer, set_global_tracer
from opentracing.mocktracer import MockTracer
from opentracing.scope_managers.tornado import TornadoScopeManager, \
    tracer_stack_context

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test


def parent_of(parent_span, *children):
    for child in children:
        assert parent_span.context.span_id == child.parent_id


def not_parent_of(parent_span, *children):
    for child in children:
        assert parent_span.context.span_id != child.parent_id


def empty_span(span, name):
    assert span.operation_name == name
    assert span.tags == {}
    assert len(span.logs) == 0


class _Base(AsyncTestCase):

    patcher_enabled = True

    def setUp(self):
        super(_Base, self).setUp()
        self.tracer = MockTracer(TornadoScopeManager())
        set_global_tracer(self.tracer)
        tornado_coroutines_opentracing.enabled = self.patcher_enabled

    def tearDown(self):
        super(_Base, self).tearDown()
        tornado_coroutines_opentracing.enabled = False


class IsCoroutineTestCase(_Base):

    def test_is_coroutine(self):

        @gen.engine
        def engine():
            pass

        @gen.coroutine
        def coroutine():
            pass

        assert gen.is_coroutine_function(engine)
        assert gen.is_coroutine_function(coroutine)


class _BaseCoroTestCase(_Base):

    decorator = None

    def setUp(self):
        super(_BaseCoroTestCase, self).setUp()
        if self.decorator:
            self.coro = self._decorator(self.coro)
            self.nested_coro = self._decorator(self.nested_coro)
            self.coros = self._decorator(self.coros)
            self.coro_exception = self._decorator(self.coro_exception)

    def _decorator(self, func):
        return getattr(gen, self.decorator)(func)

    def wait_finished_spans(self, count, timeout=5.0):
        @gen.coroutine
        def wait():
            while len(self.tracer.finished_spans()) < count:
                yield gen.moment

        self.io_loop.run_sync(wait, timeout)
        finished_spans = self.tracer.finished_spans()
        assert len(finished_spans) == count
        return finished_spans

    def coro(self, name='coro'):
        yield gen.sleep(0.1)
        with global_tracer().start_active_span(name):
            yield gen.sleep(0.1)

    def coros(self):
        yield self.coro('coro 1')
        yield self.coro('coro 2')

    def nested_coro(self):
        with global_tracer().start_active_span('coro'):
            yield self.coro('nested')

    def coro_exception(self):
        with global_tracer().start_active_span('coro'):
            raise Exception('foobar')

    def test_fire_and_forget_single_coro(self):

        with global_tracer().start_active_span('root'):
            self.coro()

        root_span, coro_span = self.wait_finished_spans(2)
        # It's ok for 'fire & forget' case that child span finishes later than
        # root span.
        assert coro_span.finish_time > root_span.finish_time

        empty_span(root_span, 'root')
        empty_span(coro_span, 'coro')
        parent_of(root_span, coro_span)

    def test_fire_and_forget_multiple_coro(self):

        with global_tracer().start_active_span('root'):
            self.coro('coro 1')
            self.coro('coro 2')

        root_span, coro1_span, coro2_span = self.wait_finished_spans(3)

        empty_span(root_span, 'root')
        empty_span(coro1_span, 'coro 1')
        empty_span(coro2_span, 'coro 2')

        parent_of(root_span, coro1_span, coro2_span)

    def test_fire_and_forget_nested_coro(self):
        with global_tracer().start_active_span('root'):
            self.nested_coro()

        root_span, nested_span, coro_span = self.wait_finished_spans(3)
        empty_span(root_span, 'root')
        empty_span(nested_span, 'nested')
        empty_span(coro_span, 'coro')

        parent_of(coro_span, nested_span)

    def test_fire_and_forget_coro_with_two_yield_inside(self):
        with global_tracer().start_active_span('root'):
            self.coros()

        root_span, coro1_span, coro2_span = self.wait_finished_spans(3)
        empty_span(root_span, 'root')
        empty_span(coro1_span, 'coro 1')
        empty_span(coro2_span, 'coro 2')

        parent_of(root_span, coro1_span, coro2_span)

    @gen_test
    def test_yield(self):

        with global_tracer().start_active_span('root'):
            yield self.coro()

        spans = self.tracer.finished_spans()
        assert len(spans) == 2

        coro_span, root_span = spans

        empty_span(root_span, 'root')
        empty_span(coro_span, 'coro')

        parent_of(root_span, coro_span)

    @gen_test
    def test_yield_exception(self):
        with self.assertRaisesRegexp(Exception, 'foobar'):
            with global_tracer().start_active_span('root'):
                yield self.coro_exception()

        spans = self.tracer.finished_spans()
        assert len(spans) == 2

        coro_span, root_span = spans

        assert root_span.tags == {'error': True}
        assert len(root_span.logs) == 1

        assert coro_span.tags == {'error': True}
        assert len(coro_span.logs) == 1

        parent_of(root_span, coro_span)

    def test_add_callback_without_manual_span_propagation(self):

        with tracer_stack_context():
            with global_tracer().start_active_span('root'):
                self.io_loop.add_callback(self.nested_coro)

        root_span, nested_span, coro_span = self.wait_finished_spans(3)

        empty_span(root_span, 'root')
        empty_span(nested_span, 'nested')
        empty_span(coro_span, 'coro')

        not_parent_of(root_span, coro_span, nested_span)
        parent_of(coro_span, nested_span)

    def test_add_callback_with_manual_span_propagation(self):

        def callback(span):
            def _callback():
                with global_tracer().scope_manager.activate(span, False):
                    self.nested_coro()
            return _callback

        with tracer_stack_context():
            with global_tracer().start_active_span('root') as root:
                self.io_loop.add_callback(callback(root.span))

        root_span, nested_span, coro_span = self.wait_finished_spans(3)

        empty_span(root_span, 'root')
        empty_span(nested_span, 'nested')
        empty_span(coro_span, 'coro')

        parent_of(root_span, coro_span)
        parent_of(coro_span, nested_span)


class CoroTestCase(_BaseCoroTestCase):

    decorator = 'coroutine'


class GenTestCase(_BaseCoroTestCase):

    # TODO: describe the difference between `gen.engine` and `gen.coroutine`
    # behaviour.

    decorator = 'engine'

    def test_fire_and_forget_nested_coro(self):

        with global_tracer().start_active_span('root'):
            self.nested_coro()

        root_span, coro_span, nested_span = self.wait_finished_spans(3)
        empty_span(root_span, 'root')
        empty_span(coro_span, 'coro')
        empty_span(nested_span, 'nested')

        parent_of(coro_span, nested_span)

    def test_add_callback_without_manual_span_propagation(self):

        with tracer_stack_context():
            with global_tracer().start_active_span('root'):
                self.io_loop.add_callback(self.nested_coro)

        root_span, coro_span, nested_span = self.wait_finished_spans(3)

        empty_span(root_span, 'root')
        empty_span(nested_span, 'nested')
        empty_span(coro_span, 'coro')

        not_parent_of(root_span, coro_span, nested_span)
        parent_of(coro_span, nested_span)

    def test_add_callback_with_manual_span_propagation(self):

        def callback(span):
            def _callback():
                with global_tracer().scope_manager.activate(span, False):
                    self.nested_coro()
            return _callback

        with tracer_stack_context():
            with global_tracer().start_active_span('root') as root:
                self.io_loop.add_callback(callback(root.span))

        root_span, coro_span, nested_span = self.wait_finished_spans(3)

        empty_span(root_span, 'root')
        empty_span(nested_span, 'nested')
        empty_span(coro_span, 'coro')

        parent_of(root_span, coro_span)
        parent_of(coro_span, nested_span)

    @gen_test
    def test_yield(self):

        with global_tracer().start_active_span('root'):
            yield self.coro()

        yield gen.sleep(0.1)

        spans = self.tracer.finished_spans()
        assert len(spans) == 2

        root_span, coro_span = spans

        empty_span(root_span, 'root')
        empty_span(coro_span, 'coro')

        parent_of(root_span, coro_span)


class GenTaskTestCase(_BaseCoroTestCase):

    @gen.engine
    def _coro(self, name, callback):
        with global_tracer().start_active_span(name):
            yield gen.sleep(0.1)
        callback()

    def coro(self, name='coro'):
        return gen.Task(self._coro, name)

    @gen.engine
    def _nested_coro(self, callback):
        with global_tracer().start_active_span('coro'):
            yield self.coro('nested')
        callback()

    def nested_coro(self):
        return gen.Task(self._nested_coro)

    @gen.engine
    def _coros(self, callback):
        yield self.coro('coro 1')
        yield self.coro('coro 2')
        callback()

    def coros(self):
        return gen.Task(self._coros)


# TODO: add test-case with disabled patcher.
