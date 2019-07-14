Tornado coroutines supporting in python opentracing
===================================================

**Warning! This feature is experimental currently and not tested well. You should think twice to use this package in production.**

`Opentracing python library <https://github.com/opentracing/opentracing-python/>`_ provides nice mechanism for tracing of `Tornado <https://github.com/tornadoweb/tornado>`_ code based on coroutines.

This library works perfectly with yield-style coroutines. But it doesn't support the situation of fire & forget coroutine.
In this case such coroutine doesn't store initial parent context and at the moment of execution it will take current context, that can be context of unrelated coroutine (e.g. concurrent coroutine) or None.
Another problem that such coroutine "steal" parent context and context manager couldn't finish the context properly.

Example 1
--------

.. code-block::

    from tornado import gen
    from opentracing import global_tracer

    @gen.corotine
    def do_someting_in_background():
        # a lot of work
        yield gen.sleep(0.5)

    ...

    # Context manager should finish root span automatically after exiting.
    # Because we don't wait for coroutine result (fire & forget), the
    # context manager will exit right after calling coroutine.
    # So root span should be finished...
    with global_tracer().start_active_span('work in background') as root:
        do_someting_in_background()

    # ...but it's not finished
    assert root.span.finished == False
