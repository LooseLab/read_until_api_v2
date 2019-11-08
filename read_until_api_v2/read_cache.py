"""read_cache.py

This module contains the read cache classes that are used to hold reads as they
are streamed from MinKNOW to the ReadUntil API.

We provide a BaseCache class that implements the required methods for retrieving
reads from the queue in a thread safe manner. It is an ordered and keyed queue,
of a maximum size, based on collections.OrderedDict.

See BaseCache for details on implementing your own cache.
"""
from collections import OrderedDict
from collections.abc import MutableMapping
from threading import RLock


__all__ = ["AccumulatingCache", "ReadCache"]


class BaseCache(MutableMapping):
    """A thread-safe dict-like container with a maximum size

    This BaseCache contains all the required methods for working as an ordered
    cache with a max size except for __setitem__, which should be user defined.

    Parameters
    ----------
    size : int
        The maximum number of items to hold

    Attributes
    ----------
    size : int
        The maximum size of the cache
    missed : int
        The number items never removed from the queue
    replaced : int
        The number of items replaced by a newer item (reads chunks replaced by a
        chunk from the same read)
    _container : OrderedDict
        An instance of an OrderedDict that forms the read cache
    lock : threading.Rlock
        The instance of the lock used to make the cache thread-safe

    Examples
    --------

    When inheriting from BaseCache only the __setitem__ method needs to be
    included. The attribute `_container` is an instance of OrderedDict that
    forms the cache so this is the object that must be updated.

    This example is not likely to be a good cache.

    >>> class DerivedCache(BaseCache):
    ...     def __setitem__(self, key, value):
    ...         # The lock is required to maintain thread-safety
    ...         with self.lock:
    ...             # Logic to apply when adding items to the cache
    ...             self._container[key] = value
    """

    def __init__(self, size):
        # Using this test instead of @abc.abstractmethod so
        #  that sub-classes don't require a __init__ method
        if self.__class__ == BaseCache:
            raise TypeError(
                "Can't instantiate abstract class {}".format(BaseCache.__name__)
            )

        if size < 1:
            raise ValueError("size must be > 1")

        self.size = size
        self.replaced = 0
        self.missed = 0
        self._container = OrderedDict()
        self.lock = RLock()

    def __getitem__(self, key):
        """Delegate with lock."""
        with self.lock:
            return self._container.get(key)

    def __setitem__(self, k, v):
        """Raise NotImplementedError if not overridden."""
        raise NotImplementedError("__setitem__ should be overridden by your cache.")

    def __delitem__(self, key):
        """Delegate with lock."""
        with self.lock:
            self._container.__delitem__(key)

    def __len__(self):
        """Delegate with lock."""
        with self.lock:
            return len(self._container)

    def __iter__(self):
        """Raise NotImplementedError as unlikely to be thread-safe."""
        raise NotImplementedError("Iteration is unlikely to be thread-safe.")

    def __contains__(self, item):
        """Delegate with lock."""
        with self.lock:
            return self._container.__contains__(item)

    def popitem(self, last=True):
        """Delegate with lock."""
        with self.lock:
            return self._container.popitem(last=last)

    def popitems(self, n=1, last=True):
        """Return a list of popped items from the cache.

        Parameters
        ----------
        n : int
            The maximum number of items to return
        last : bool
           If True, return the newest entries (LIFO); if False oldest entries (FIFO)

        Returns
        -------
        list
            Output list of upto n (key, value) pairs from the cache
        """
        if n > self.size:
            n = self.size

        with self.lock:
            data = []
            while len(self._container) > self.size - n:
                data.append(self._container.popitem(last=last))

        return data

    def keys(self):
        """Return a list of keys currently in the cache."""
        with self.lock:
            return list(self._container.keys())


class ReadCache(BaseCache):
    def __setitem__(self, key, value):
        with self.lock:
            already_replaced = False
            while len(self._container) >= self.size:
                already_replaced = True
                k, v = self._container.popitem(last=False)
                if k == key and v.number == value.number:
                    self.replaced += 1
                else:
                    self.missed += 1

            if key in self._container:
                if not already_replaced:
                    if self._container[key].number == value.number:
                        self.replaced += 1
                    else:
                        self.missed += 1

                self._container.__delitem__(key)
            self._container.__setitem__(key, value)


class AccumulatingCache(BaseCache):
    def __setitem__(self, key, value):
        with self.lock:
            if key not in self._container:
                # Key not in dict
                self._container.__setitem__(key, value)
            else:
                # Key exists
                if self[key].number == value.number:
                    # Same read, so update raw data
                    self[key].raw_data += value.raw_data
                    self.missed += 1
                else:
                    # New read
                    self._container.__setitem__(key, value)
                    self.replaced += 1
            # Move most recently updated keys to right
            # TODO: test moving the other way e.g left,
            #  shouldn't make too much of a difference.
            self._container.move_to_end(key)
