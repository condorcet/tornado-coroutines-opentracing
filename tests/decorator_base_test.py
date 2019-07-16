# coding: utf-8
from tornado import gen
from tornado_coroutines_opentracing import ff_coroutine

from . import _Base


class BaseDecoratorTestCase(_Base):

    def test_is_coroutine(self):
        @ff_coroutine
        def coroutine():
            pass

        @ff_coroutine
        @gen.coroutine
        def wrapped():
            pass

        @ff_coroutine
        @ff_coroutine
        def double_wrapped():
            pass

        assert gen.is_coroutine_function(coroutine) is True
        assert gen.is_coroutine_function(wrapped) is True
        assert gen.is_coroutine_function(double_wrapped) is True

    def test_non_async_coroutine(self):
        """
        Synchronous coroutine without yield, must return Future with the
        result instantly.
        """

        @gen.coroutine
        def coro(value):
            return value

        decorated_coro = ff_coroutine(coro)

        # Original coroutine.
        fut = coro(42)
        assert fut.done() is True
        assert fut.result() == 42

        # Once again with wrapped coroutine.
        fut = decorated_coro('foobar')
        assert fut.done() is True
        assert fut.result() == 'foobar'

    def test_no_need_additional_step_in_event_loop(self):
        """
        Execution of wrapped coroutine must be scheduled on current iteration
        of event loop, rather than on next.
        """

        operations = []

        def add_result(op):
            operations.append(op)
            if len(operations) == 4:
                self.stop()

        def callback(op):
            add_result(op)

        @gen.coroutine
        def coro(op):
            add_result(op)

        decorated_coro = ff_coroutine(coro)

        # First add coroutine on next iteration and after -- callback.
        self.io_loop.add_callback(coro, 1)
        self.io_loop.add_callback(callback, 2)
        # Do the same but with wrapped coroutine.
        self.io_loop.add_callback(decorated_coro, 3)
        self.io_loop.add_callback(callback, 4)
        self.wait()

        # If execution of decorated_coro had taken extra iteration, then
        # callback (4) would have executed earlier.
        assert operations == [1, 2, 3, 4, ]
