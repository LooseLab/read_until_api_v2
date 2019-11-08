import time
import logging
from itertools import count as _count
from multiprocessing import TimeoutError
from multiprocessing.pool import ThreadPool

_counter = _count()


def new_thread_name(template="read_until-{:04d}"):
    """Helper to generate new thread names"""
    return template.format(next(_counter))


def setup_logger(name, log_format="%(message)s", log_file=None, mode="a", level=logging.DEBUG):
    """Setup loggers

    Parameters
    ----------
    name : str
        Name to give the logger
    log_format : str
        logging format string using % formatting
    log_file : str
        File to record logs to, sys.stderr if not set
    mode : str
        If log_file is specified, open the file in this mode. Defaults to 'a'
    level : logging.LEVEL
        Where logging.LEVEL is one of (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns
    -------
    logger
    """
    formatter = logging.Formatter(log_format)
    if log_file is not None:
        handler = logging.FileHandler(log_file, mode=mode)
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False

    return logger


def nice_join(seq, sep=", ", conjunction="or"):
    """Join lists nicely"""
    seq = [str(x) for x in seq]

    if len(seq) <= 1 or conjunction is None:
        return sep.join(seq)
    else:
        return "{} {} {}".format(sep.join(seq[:-1]), conjunction, seq[-1])


def _import(name):
    """Dynamically import modules and classes, used to get the ReadCache

    https://stackoverflow.com/a/547867/3279716
    https://docs.python.org/2.4/lib/built-in-funcs.html

    Parameters
    ----------
    name : str
        The module/class path. E.g: "read_until.read_cache.{}".format("ReadCache")

    Returns
    -------
    module
    """
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def run_workflow(client, partial_analysis_func, n_workers, run_time, runner_kwargs=None):
    """Run an analysis function against a ReadUntilClient

    Parameters
    ----------
    client : read_until.ReadUntilClient
        An instance of the ReadUntilClient object
    partial_analysis_func : partial function
        Analysis function to process reads, should
        exit when client.is_running == False
    n_workers : int
        Number of analysis worker functions to run
    run_time : int
        Time, in seconds, to run the analysis for
    runner_kwargs : dict
        Keyword arguments to pass to client.run()

    Returns
    -------
    list
        Results from the analysis function, one item per worker

    """
    if runner_kwargs is None:
        runner_kwargs = dict()

    logger = logging.getLogger("Manager")

    results = []
    pool = ThreadPool(n_workers)
    logger.info("Creating {} workers".format(n_workers))
    try:
        # start the client
        client.run(**runner_kwargs)
        # start a pool of workers
        for _ in range(n_workers):
            results.append(pool.apply_async(partial_analysis_func))
        pool.close()
        # wait a bit before closing down
        time.sleep(run_time)
        logger.info("Sending reset")
        client.reset()
        pool.join()
    except KeyboardInterrupt:
        logger.info("Caught ctrl-c, terminating workflow.")
        client.reset()
    except Exception:
        client.reset()
        raise

    # collect results (if any)
    collected = []
    for result in results:
        try:
            res = result.get(5)
        except TimeoutError:
            logger.warning("Worker function did not exit successfully.")
            # collected.append(None)
        except Exception as e:
            logger.exception("EXCEPT", exc_info=e)
            # logger.warning("Worker raise exception: {}".format(repr(e)))
        else:
            logger.info("Worker exited successfully.")
            collected.append(res)
    pool.terminate()
    return collected
