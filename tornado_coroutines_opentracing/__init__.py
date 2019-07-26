# coding: utf-8
import functools
from tornado import gen
from opentracing import global_tracer
from opentracing.scope_managers.tornado import tracer_stack_context


original_gen_coroutine = gen.coroutine


def ff_coroutine(func_or_coro):
    """
    Декоратор над Торнадо-корутиной (`gen.coroutine`) позволяющий выполнять
    корутину по принципу fire & forget сохраняя родительский контекста спана
    в рамках которого эта корутина была вызвана:
    ```
        from opentracing import global_tracer

        @ff_coroutine
        @gen.coroutine
        def coro():
            ...
            with global_tracer().start_active_span(
                operation_name='child,
                # будет взят родительский спан
                child_of=global_tracer().active_span
            ):
                # do something
                pass

        ...

        with global_tracer().start_active_span('root'):
            coro()
    ```

    Подход работает в случае непосредственного вызова корутины, но не работает
    для запланированных в event loop-е колбэков:
    ```
        IOLoop.instance().add_callback(coro)
    ```

    В настоящее время, контекстный менеджер `opentracing.TornadoScopeManager`
    (или торнадовский StackContext на котором он построен?) не позволяет
    правильно сохранять родительский контекст для корутин, которые будут
    вызваны без возвращения управления (без yield) (см. также замечание в
    https://github.com/opentracing/opentracing-python/blob/f6bcb0aad81ec9d89414
    3148612312bd48a02a91/opentracing/scope_managers/tornado.py#L64).

    В связи с изменениями, следует помнить о следующем:

    1) Время открытия и завершения дочернего спан в корутине может быть позже
       завершения родительского спана, что нормально:
    ```
    --------------------------------------------------> время
         * parent span *
               |
               | -> * child span * (завершена позже родительского)
               |
                ------------> * child span * (начата и завершена позже)
    ```

    2) Осторожно использовать декоратор в случае рекурсивных вызовов корутины
       (например с целью периодичного полинга), порождающей дочерний спан.
       Это может привести к бесконечному росту дочерних спанов и стэку
       контекстов (`tracer_stack_context`):
    ```
    --------------------------------------------------> время
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

        span = global_tracer().active_span

        @original_gen_coroutine
        @functools.wraps(coro)
        def _coro(*args, **kwargs):
            exc = None
            if span:
                with global_tracer().scope_manager.activate(span, False):
                    try:
                        res = yield coro(*args, **kwargs)
                    except Exception as e:
                        # Catch all exceptions but raise them out of scope to
                        # avoid logging errors twice while yielding coroutine.
                        exc = e
                if exc:
                    raise exc
            else:
                res = yield coro(*args, **kwargs)
            raise gen.Return(res)

        with tracer_stack_context():
            return _coro(*args, **kwargs)

    _func.__ff_traced_coroutine__ = True

    # Return function that looks like Tornado coroutine.
    _func.__wrapped__ = coro.__wrapped__
    _func.__tornado_coroutine__ = True
    return _func
