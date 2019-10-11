Supporting fire & forget Tornado coroutines in python opentracing
=================================================================

**Warning! This feature is experimental currently and not tested well. You should think twice to use this package in production.**

`Opentracing python library <https://github.com/opentracing/opentracing-python/>`_ provides nice mechanism for tracing of `Tornado <https://github.com/tornadoweb/tornado>`_ code based on coroutines.

Coroutine must be invoked with `tracer_stack_context` that allows to isolate context and using parent scope in child corutines. It works only when you yielding child coroutines inside, but doesn't work with fire & forget coroutine.
In this case such coroutine can lose parent scope while yielding or take scope of unrelated coroutine (e.g. concurrent coroutines).

To avoid it you have to wrap coroutine by `tracer_stack_context` manually, and activate parent scope that you need right inside coroutine:

.. code-block::

    from tornado import gen
    from opentracing import global_tracer
    from opentracing.scope_managers.tornado import tracer_stack_context

    @gen.corotine
    def do_someting_in_background(parent):
        with global_tracer().scope_manager.activate(parent, False):
            with global_tracer.start_active_span('do something', True):
                yield gen.sleep(0.5)
                # do something

    ...


    with global_tracer().start_active_span('work in background') as root:
        with tracer_stack_context():
            do_someting_in_background(root.span)


ff_coroutine
------------

This library provides `ff_coroutine` decorator that does it for you:

.. code-block::

    from opentracing import global_tracer
    from tornado_coroutines_opentracing import ff_coroutine

    @ff_coroutine
    def do_someting_in_background():
        with global_tracer.start_active_span('do something', True):
            yield gen.sleep(0.5)
            # do something

    ...


    with global_tracer().start_active_span('work in background'):
        do_someting_in_background()


It also works with nested coroutines:

.. code-block::

    from opentracing import global_tracer
    from tornado_coroutines_opentracing import ff_coroutine

    @ff_coroutine
    def bar():
        with global_tracer.start_active_span('bar', True):
            # do something

    @ff_coroutine
    def foo():
        with global_tracer.start_active_span('foo', True):
            yield gen.sleep(0.5)
            bar()


    ...

    with global_tracer().start_active_span('work in background'):
        foo()


`ff_coroutine` yielded as well as Tornado coroutine (via `gen.coroutine`):

.. code-block::

    from opentracing import global_tracer
    from tornado_coroutines_opentracing import ff_coroutine

    @ff_coroutine
    def bar():
        with global_tracer.start_active_span('bar', True):
            # do something

    @ff_coroutine
    def foo():
        with global_tracer.start_active_span('foo', True):
            yield bar()
            yield gen.sleep(0.5)


    ...

    with global_tracer().start_active_span('work in background'):
        yield foo()


Sometimes you want to disable tracing in your application. You can disable `ff_coroutine` too:

.. code-block::

    from tornado_coroutines_opentracing import State
    ...

    State.enabled = False


