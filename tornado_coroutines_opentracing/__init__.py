# coding: utf-8
import functools
from tornado import gen
from opentracing import global_tracer
from opentracing.scope_managers.tornado import tracer_stack_context

enabled = True


original_gen_engine = gen.engine
original_gen_coroutine = gen.coroutine


def span_decorator(func, decorator):

    @decorator
    def _func(*args, **kwargs):
        span = kwargs.pop('__span')
        if span:
            with global_tracer().scope_manager.activate(span, False):
                yield func(*args, **kwargs)
        else:
            yield func(*args, **kwargs)
    return _func


def wrap_decorator(decorator):

    def wrapped(func):

        coro_func = decorator(func)
        func = span_decorator(coro_func, decorator)

        @functools.wraps(func)
        def _func(*args, **kwargs):
            span = global_tracer().active_span
            if span and enabled:
                with tracer_stack_context():
                    return func(*args, __span=span, **kwargs)
            else:
                return func(*args, __span=None, **kwargs)

        # As well as original decorator the wrapper should return a coroutine.
        _func.__wrapped__ = coro_func.__wrapped__
        _func.__tornado_coroutine__ = True
        return _func

    return wrapped


gen.engine = wrap_decorator(original_gen_engine)
gen.coroutine = wrap_decorator(original_gen_coroutine)
