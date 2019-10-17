# coding: utf-8
import functools
from tornado import gen
from opentracing import global_tracer
from opentracing.scope_managers.tornado import _TracerRequestContext,\
    ThreadSafeStackContext, _TracerRequestContextManager, _TornadoScope


original_gen_coroutine = gen.coroutine


class State:
    enabled = True


def tracer_stack_context(parent_span=None):
    """
    Replacement of original `tracer_stack_context` from OpenTracing.
    It allows to specify span in new stack context that will be parent for
    children spans.
    """
    # TODO: remove this when the feature be released
    # https://github.com/opentracing/opentracing-python/pull/126
    if parent_span is not None:
        scope = _TornadoScope(
            global_tracer().scope_manager, parent_span, False)
    else:
        scope = None
    context = _TracerRequestContext(scope)
    return ThreadSafeStackContext(
        lambda: _TracerRequestContextManager(context)
    )


def ff_coroutine(func_or_coro):
    """
    Extended `gen.coroutine` decorator that provides to fire & forget coroutine
    without losing parent scope while yielding it:
    ```
        from opentracing import global_tracer

        @ff_coroutine
        @gen.coroutine
        def coro():
            ...
            with global_tracer().start_active_span(
                operation_name='child,
                # parent span will be taken
                child_of=global_tracer().active_span
            ):
                # do something
                pass

        ...

        with global_tracer().start_active_span('root'):
            coro()
    ```

    Should remember following things:

    1) Child spans could be started and finished later than parent span had
    been finished. It's expected behaviour:
    ```
    --------------------------------------------------> time
         * parent span *
               |
               | -> * child span *
               |
                ------------> * child span *
    ```

    2) Decorator should be used carefully with recursive coroutines. It can
    lead to endless growth of child spans and stack contexts:
    ```
    --------------------------------------------------> time
         * parent span *
               |
                --> * child 1 *
                        |
                         --> * child 2 *
                                 |
                                  --> * child 3 *
                                          |
                                           ...
    ```

    """

    if hasattr(func_or_coro, '__ff_traced_coroutine__'):
        return func_or_coro
    if not gen.is_coroutine_function(func_or_coro):
        coro = original_gen_coroutine(func_or_coro)
    else:
        coro = func_or_coro

    @functools.wraps(coro)
    def _func(*args, **kwargs):
        if not State.enabled:
            return coro(*args, **kwargs)

        with tracer_stack_context(global_tracer().active_span):
            return coro(*args, **kwargs)

    _func.__ff_traced_coroutine__ = True

    # Return function that looks like Tornado coroutine.
    _func.__wrapped__ = coro.__wrapped__
    _func.__tornado_coroutine__ = True
    return _func
