# simple retries decorator

def retry(times):
    def try_it(func):
        def f(*fargs, **fkwargs):
            attempts = 0
            exc = None
            while attempts < times:
                try:
                    return func(*fargs, **fkwargs)
                except Exception as e:
                    attempts += 1
                    exc = e
            raise exc
        return f
    return try_it
