# coding: utf-8
from tornado_coroutines_opentracing import ff_coroutine

from opentracing import global_tracer
from opentracing.scope_managers.tornado import tracer_stack_context

from tornado import gen

from . import _Base, is_parent_of, empty_span, has_no_parent, has_exception


class FireAndForgetTestCase(_Base):

    """
    Test-case about fire & forget coroutines.

    NB: almost each test make root span which context will be taken by child
    coroutine. After invoking coroutine as fire & forget, context manager will
    close root span. That's why root is the first span in finished spans.
    """

    @gen.coroutine
    def coro(self, name):
        yield gen.sleep(0.1)
        with global_tracer().start_active_span(
                operation_name=name,
                child_of=global_tracer().active_span
        ):
            yield gen.sleep(0.1)

    def test_without_spans(self):
        """
        Отсутствие корневого спана не ломает поведение корутины.
        """

        def callback(fut):
            self.stop(fut.result())

        @ff_coroutine
        def coro(value):
            raise gen.Return(value)

        fut = coro(42)
        fut.add_done_callback(callback)
        assert self.wait() == 42

        assert len(global_tracer().finished_spans()) == 0

    def test_without_root_span(self):

        def callback(fut):
            self.stop(fut.result())

        @ff_coroutine
        def coro(value):
            with global_tracer().start_active_span(
                    operation_name='coro',
            ):
                pass
            raise gen.Return(value)

        fut = coro(42)
        fut.add_done_callback(callback)
        assert self.wait() == 42

        coro, = global_tracer().finished_spans()
        assert empty_span(coro, 'coro')

    def test_with_yield(self):

        @ff_coroutine
        def coro():
            yield gen.sleep(0.1)

        with global_tracer().start_active_span('root'):
            coro()

        root, = self.wait_finished_spans(1)
        assert empty_span(root, 'root')

    def test_with_multiple_yield(self):
        """
        Yielding several times in coroutine.
        """
        @ff_coroutine
        def coro():
            yield gen.sleep(0.1)
            yield gen.sleep(0.1)

        with global_tracer().start_active_span('root'):
            coro()

        root, = self.wait_finished_spans(1)
        assert empty_span(root, 'root')

    def test_make_own_span_with_multiple_yield_different_contexts(self):

        """
        Open child span in invoked coroutine.
        """

        @ff_coroutine
        def coro():
            yield gen.sleep(0.1)  # switch execution on next iteration of loop.
            with global_tracer().start_active_span(
                    operation_name='fire_and_forget',
                    child_of=global_tracer().active_span
            ):
                # ... return execution to couroutine in new child scope.
                yield gen.sleep(0.1)

        with global_tracer().start_active_span('root'):
            coro()

        root, ff = self.wait_finished_spans(2)
        assert empty_span(root, 'root')
        assert empty_span(ff, 'fire_and_forget')
        assert is_parent_of(root, ff)

    def test_with_yield_coro(self):

        """
        In first couroutine yield another one, that opens the child span.
        """
        @ff_coroutine
        def coro():
            yield self.coro('coro')

        with global_tracer().start_active_span('root'):
            coro()

        root, coro = self.wait_finished_spans(2)
        assert empty_span(root, 'root')
        assert empty_span(coro, 'coro')
        assert is_parent_of(root, coro)

    def test_make_own_span_with_yield_coro(self):

        """
        First coroutine opens the child span, in which scope we yielding
        another one that opens new child scope.
        """

        @ff_coroutine
        def coro():
            with global_tracer().start_active_span(
                    operation_name='fire_and_forget',
                    child_of=global_tracer().active_span
            ):
                yield self.coro('coro')

        with global_tracer().start_active_span('root'):
            coro()

        root, coro, ff = self.wait_finished_spans(3)
        assert empty_span(root, 'root')
        assert empty_span(coro, 'coro')
        assert empty_span(ff, 'fire_and_forget')

        assert is_parent_of(root, ff)
        assert is_parent_of(ff, coro)

    def test_with_multiple_yield_coro(self):
        """
        Yielding coroutines in invoked coroutine that open child spans.
        """
        @ff_coroutine
        def coro():
            yield self.coro('coro_1')
            yield self.coro('coro_2')

        with global_tracer().start_active_span('root'):
            coro()

        root, coro_1, coro_2 = self.wait_finished_spans(3)
        assert empty_span(root, 'root')
        assert empty_span(coro_1, 'coro_1')
        assert empty_span(coro_2, 'coro_2')

        assert is_parent_of(root, coro_1, coro_2)

    def test_make_own_span_with_multiple_yield_coro(self):

        """
        Open child span inside invoked coroutine and yielding several
        coroutines that also opening child spans.
        """

        @ff_coroutine
        def coros(*names):
            with global_tracer().start_active_span(
                    operation_name='fire_and_forget',
                    child_of=global_tracer().active_span
            ):
                for name in names:
                    yield self.coro(name)

        with global_tracer().start_active_span('root'):
            coros('coro_1', 'coro_2')

        root, coro_1, coro_2, ff = self.wait_finished_spans(4)

        assert empty_span(root, 'root')
        assert empty_span(coro_1, 'coro_1')
        assert empty_span(coro_2, 'coro_2')
        assert empty_span(ff, 'fire_and_forget')

        assert is_parent_of(root, ff)
        assert is_parent_of(ff, coro_1)
        assert is_parent_of(ff, coro_2)

    def test_call_fire_and_forget_coro(self):

        """
        Inside coroutine fire & forget another one that makes child span.
        """

        @ff_coroutine
        def second_coro():
            yield gen.sleep(0.1)
            with global_tracer().start_active_span(
                    operation_name='second_coro',
                    child_of=global_tracer().active_span
            ):
                yield gen.sleep(0.1)

        @ff_coroutine
        def first_coro():
            second_coro()

        with global_tracer().start_active_span('root'):
            first_coro()

        root, second = self.wait_finished_spans(2)
        assert empty_span(root, 'root')
        assert empty_span(second, 'second_coro')

        assert is_parent_of(root, second)

    def test_make_own_span_call_and_fire_and_forget_coro(self):

        """
        Make their own child spans in each fire & forget coroutine.
        """

        @ff_coroutine
        def second_coro():
            yield gen.sleep(0.1)
            with global_tracer().start_active_span(
                    operation_name='second_coro',
                    child_of=global_tracer().active_span
            ):
                yield gen.sleep(0.1)

        @ff_coroutine
        def first_coro():
            with global_tracer().start_active_span(
                    operation_name='first_coro',
                    child_of=global_tracer().active_span
            ):
                second_coro()
                yield gen.sleep(0.1)

        with global_tracer().start_active_span('root'):
            first_coro()

        root, first, second = self.wait_finished_spans(3)
        assert empty_span(root, 'root')
        assert empty_span(first, 'first_coro')
        assert empty_span(second, 'second_coro')

        assert is_parent_of(root, first)
        assert is_parent_of(first, second)

    def test_multiple_fire_and_forget(self):

        """
        Fire & forget several coroutines that make their own child spans.
        """

        @ff_coroutine
        def coro(name):
            yield self.coro(name)

        with global_tracer().start_active_span('root'):
            for name in ('coro_1', 'coro_2'):
                coro(name)

        root, coro_1, coro_2 = self.wait_finished_spans(3)

        assert empty_span(root, 'root')
        assert empty_span(coro_1, 'coro_1')
        assert empty_span(coro_2, 'coro_2')

        assert is_parent_of(root, coro_1, coro_2)

    def test_coro_multiple_fire_and_forget_make_own_span(self):

        """
        Invoke coroutine with child span and fire & forget several coroutines
        with their own child spans inside.
        """

        @ff_coroutine
        def coro(name):
            yield self.coro(name)

        @ff_coroutine
        def coros(*names):
            with global_tracer().start_active_span(
                    operation_name='fire_and_forget',
                    child_of=global_tracer().active_span
            ):
                yield gen.sleep(0.1)
                for name in names:
                    coro(name)

        with global_tracer().start_active_span('root'):
            coros('coro_1', 'coro_2')

        root, ff, coro_1, coro_2 = self.wait_finished_spans(4)

        assert empty_span(root, 'root')
        assert empty_span(ff, 'fire_and_forget')
        assert empty_span(coro_1, 'coro_1')
        assert empty_span(coro_2, 'coro_2')

        assert is_parent_of(root, ff)
        assert is_parent_of(ff, coro_1, coro_2)

    def test_coro_multiple_fire_and_forget_make_own_span_per_each_coro(self):

        """
        Invoke coroutine with child span and fire & forget several others
        with their own child spans inside.
        """

        @ff_coroutine
        def coro(name):
            yield self.coro(name)

        @ff_coroutine
        def coros(*names):
            yield gen.sleep(0.1)
            for name in names:
                with global_tracer().start_active_span(
                        operation_name='parent:{}'.format(name),
                        child_of=global_tracer().active_span
                ):
                    coro(name)

        with global_tracer().start_active_span('root'):
            coros('coro_1', 'coro_2')

        root, parent_1, parent_2, coro_1, coro_2 = self.wait_finished_spans(5)

        assert empty_span(root, 'root')
        assert empty_span(parent_1, 'parent:coro_1')
        assert empty_span(coro_1, 'coro_1')
        assert empty_span(parent_2, 'parent:coro_2')
        assert empty_span(coro_2, 'coro_2')

        assert is_parent_of(root, parent_1, parent_2)
        assert is_parent_of(parent_1, coro_1)
        assert is_parent_of(parent_2, coro_2)

    def test_recursion_coro(self):
        """
        Child span in recursive called coroutine has parent span from previous
        recursion step.
        """

        count = 3

        @ff_coroutine
        def coro(n=1):
            yield gen.moment
            with global_tracer().start_active_span(
                    operation_name=str(n),
                    child_of=global_tracer().active_span
            ):
                if n < count:
                    coro(n+1)

        with global_tracer().start_active_span('0'):
            coro()

        spans = self.wait_finished_spans(count + 1)

        assert empty_span(spans[0], '0')
        for i in range(0, count):
            assert empty_span(spans[i], str(i))
            assert is_parent_of(spans[i], spans[i+1])

    def test_recursion_break_parent_context(self):
        """
        Break parent span with using `tracer_stack_context` manager in
        recursion.
        """

        count = 4

        @ff_coroutine
        def coro(n=1):
            yield gen.moment
            with global_tracer().start_active_span(
                    operation_name=str(n),
                    child_of=global_tracer().active_span
            ):
                if n < count/2:
                    coro(n+1)
                elif n < count:
                    with tracer_stack_context():
                        # clear current scope.
                        coro(n+1)

        with global_tracer().start_active_span('root'):
            coro()

        root, s1, s2, s3, s4 = self.wait_finished_spans(count + 1)
        assert empty_span(root, 'root')
        assert is_parent_of(root, s1)

        assert empty_span(s1, '1')
        assert is_parent_of(s1, s2)

        assert empty_span(s2, '2')

        assert empty_span(s3, '3')
        assert has_no_parent(s3)

        assert empty_span(s4, '4')
        assert has_no_parent(s4)


class FireAndForgetExceptionsTestCase(_Base):

    """
    Test-case about exceptions in fire & forget coroutines

    Exceptions from such coroutines must NOT propagate to their parent spans.
    """

    @gen.coroutine
    def coro(self, exc):
        raise exc

    @gen.coroutine
    def coro_with_span(self, name, exc):
        with global_tracer().start_active_span(
                operation_name=name,
                child_of=global_tracer().active_span
        ):
            raise exc

    def test_exceptions_without_any_spans(self):

        def callback(fut):
            self.stop(fut.exception())

        @ff_coroutine
        def coro(exc):
            raise exc

        exc = Exception('foobar')

        fut = coro(exc)
        fut.add_done_callback(callback)

        assert self.wait() == exc

        assert len(global_tracer().finished_spans()) == 0

    def test_exceptions_without_root_span(self):

        def callback(fut):
            self.stop(fut.exception())

        @ff_coroutine
        def coro(exc):
            with global_tracer().start_active_span(
                    operation_name='coro',
                    child_of=global_tracer().active_span
            ):
                raise exc

        exc = Exception('foobar')

        fut = coro(exc)
        fut.add_done_callback(callback)

        assert self.wait() == exc

        coro, = global_tracer().finished_spans()
        assert has_exception(coro, 'coro', exc)

    def test_exception_in_root_span(self):

        @ff_coroutine
        def coro():
            raise Exception('foobar')

        with global_tracer().start_active_span('root'):
            coro()

        root, = self.wait_finished_spans(1)
        assert empty_span(root, 'root')

    def test_exception_in_own_span_while_yielded_coro(self):

        @ff_coroutine
        def coro(exc):
            with global_tracer().start_active_span(
                    operation_name='coro',
                    child_of=global_tracer().active_span
            ):
                yield gen.sleep(0.1)
                yield self.coro(exc)

        exc = Exception('foobar')

        with global_tracer().start_active_span('root'):
            coro(exc)

        root, coro = self.wait_finished_spans(2)
        assert empty_span(root, 'root')

        assert coro.operation_name == 'coro'
        assert coro.tags == {'error': True}
        assert len(coro.logs) == 1
        assert coro.logs[0].key_values['error.object'] == exc

    def test_exception_while_yielded_coro_with_own_span(self):

        @ff_coroutine
        def coro(exc):
            yield gen.sleep(0.1)
            yield self.coro_with_span('coro', exc)

        exc = Exception('foobar')

        with global_tracer().start_active_span('root'):
            coro(exc)

        root, coro = self.wait_finished_spans(2)

        assert empty_span(root, 'root')

        assert has_exception(coro, 'coro', exc)

    def test_exception_while_fire_and_forget_another_coroutine(self):

        @ff_coroutine
        def coro_exception(exc):
            with global_tracer().start_active_span(
                    operation_name='coro_exception',
                    child_of=global_tracer().active_span
            ):
                yield gen.sleep(0.1)
                raise exc

        @ff_coroutine
        def coro(exc):
            with global_tracer().start_active_span(
                    operation_name='coro',
                    child_of=global_tracer().active_span
            ):
                yield gen.sleep(0.1)
                coro_exception(exc)

        exc = Exception('foobar')

        with global_tracer().start_active_span('root'):
            coro(exc)

        root, coro, coro_exc = self.wait_finished_spans(3)

        assert empty_span(root, 'root')
        assert empty_span(coro, 'coro')

        assert has_exception(coro_exc, 'coro_exception', exc)
