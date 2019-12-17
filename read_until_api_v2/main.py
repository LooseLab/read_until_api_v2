#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Main module.

This is a python3 implementation of the read_until_api found at
https://github.com/nanoporetech/read_until_api/

It enables the user to run Read Until in a virtual environment and doesn't
require installing within the MinKNOW directory.

"""
# Core lib
import logging
import queue
import sys
import time
import uuid
from collections import defaultdict, Counter
from pathlib import Path
from threading import Event, Thread

# Pypi packages
import numpy as np

# Internal modules
from read_until_api_v2.load_minknow_rpc import get_rpc_connection, parse_message
from read_until_api_v2.utils import nice_join, _import, setup_logger, new_thread_name


__all__ = ["ReadUntilClient"]

# This replaces the results of an old call to MinKNOWs
#  jsonRPC interface. That interface does not respond
#  correctly when a run has been configured using the
#  newer gRPC interface. This information is not currently
#  available with the gRPC interface so as a temporary
#  measure we list a standard set of values here.
CLASS_MAP = {
    "read_classification_map": {
        83: "strand",
        67: "strand1",
        77: "multiple",
        90: "zero",
        65: "adapter",
        66: "mux_uncertain",
        70: "user2",
        68: "user1",
        69: "event",
        80: "pore",
        85: "unavailable",
        84: "transition",
        78: "unclassed",
    }
}


# The maximum allowed minimum read chunk size. Filtering of small read chunks
#  from the gRPC stream is buggy. The value 0 effectively disables the filtering
#  functionality.
ALLOWED_MIN_CHUNK_SIZE = 0


class ReadUntilClient:
    """A Read Until Client for interacting with MinKNOW

    The class handles basic interactions with MinKNOW's gRPC stream and provides
    a thread-safe queue, ReadCache, containing the latest data from each channel

    Attributes
    ----------
    is_running
    missed_chunks
    missed_reads
    queue_length
    acquisition_progress
    connection : read_until.rpc.Connection
        The gRPC connection to MinKNOW

    Methods
    -------
    run()
    reset()
    get_read_chunks()
    unblock_read()
    stop_receiving_read()

    """

    def __init__(
        self,
        mk_host="127.0.0.1",
        mk_port=9501,
        device=None,
        cache_size=512,
        cache_type="ReadCache",
        signal_calibration=False,
        filter_strands=True,
        one_chunk=True,
        pre_filter_classes=None,
        reload_rpc=True,
        log_file=None,
    ):
        """A basic Read Until client.

        This class handles the interactions with the MinKNOW gRPC stream.
        It requires a thread-safe queue/cache to operate. There are two
        prodived in `read_cache.py`.

        Parameters
        ----------
        mk_host : str
            The host to connect to MinKNOW on, default: "127.0.0.1"
        mk_port : int
            The insecure channel port for MinKNOW, default: 9501
        device : str
            The device to get the connection for. E.G MinION: MN18458, GridION:
            GA10000, PromethION: 1-A1-D1
        cache_size : int
            The maximum size of the read cache, default: 512
        cache_type : str or class
            The cache for managing incoming data from the gRPC.
            If a string is provided that cache will be loaded from read_cache.py.
            Otherwise, if a class is provided it will be used as the cache. See
            read_cache.py for descriptions and requirements of the cache.
        signal_calibration : bool
            Request calibrated or uncalibrated signal from the gRPC, default: False
        filter_strands : bool
            Filter incoming data for only strand like classifications. If True
            strand classes must be provided in pre_filter_classes, default: True
        one_chunk : bool
            default: True
        pre_filter_classes : set (or iterable)
            Classes to filter reads by. Ignored if `filter_strands` is False,
            default: {'strand', 'adapter'}
        reload_rpc : bool
            Repload the RPC when initiating the client, default: True
        log_file : str
            Filepath to log messages to if not provided use console, default: None

        Examples
        --------

        To set up and use a client:

        >>> read_until_client = ReadUntilClient()

        This creates an initial connection to a MinKNOW instance in
        preparation for setting up live reads stream. To initiate the stream:

        >>> read_until_client.run()

        The client is now recieving data and can send calls to methods
        of `read_until_client` can then be made in a separate thread.
        For example an continually running analysis function can be
        submitted to the executor as:

        >>> def analysis(client, *args, **kwargs):
        ...     while client.is_running:
        ...         for channel, read in client.get_read_chunks():
        ...             raw_data = np.fromstring(read.raw_data, client.signal_dtype)
        ...             # do something with raw data... and maybe call:
        ...             #    client.stop_receiving_read(channel, read.number)
        ...             #    client.unblock_read(channel, read.number)
        >>> with ThreadPoolExecutor() as executor:
        ...     executor.submit(analysis_function, read_until_client)

        To stop processing the gRPC read stream:

        >>> read_until_client.reset()

        If an analysis function is set up as above in response to
        `client.is_running`, calling the above call will cause the
        analysis function to return.

        """
        # TODO: infer flowcell size from device we get back for cache size
        #  eg: cache_size becomes "infer" or int
        #  c = self.connection
        #  len(parse_message(c.device.get_channels_layout())['channel_records'])
        if pre_filter_classes is None:
            pre_filter_classes = {"strand", "adapter"}

        self.logger = setup_logger(
            __name__,
            # "ReadUntilClient_v2",
            log_file=log_file,
            log_format="%(asctime)s %(name)s %(message)s",
            level=logging.INFO,
        )
        self.device = device
        self.mk_host = mk_host
        self.mk_port = mk_port
        self.reload_rpc = reload_rpc
        self.cache_size = cache_size

        # Alternatively, check that cache is sub of BaseCache
        if isinstance(cache_type, str):
            current_package = vars(sys.modules[__name__])["__package__"]
            self.CacheType = _import(
                "{}.read_cache.{}".format(current_package, cache_type)
            )
        else:
            self.CacheType = cache_type

        self.filter_strands = filter_strands
        self.one_chunk = one_chunk
        self.pre_filter_classes = pre_filter_classes

        if self.filter_strands and not self.pre_filter_classes:
            raise ValueError("Read filtering set but no filter classes given.")

        self.logger.info(
            "Client type: {} chunk".format("single" if self.one_chunk else "many")
        )
        self.logger.info("Cache type: {}".format(self.CacheType.__name__))

        pre_filter_classes_str = "no filter"
        if self.pre_filter_classes:
            pre_filter_classes_str = nice_join(self.pre_filter_classes, " ", "and")

        self.logger.info("Filter for classes: {}".format(pre_filter_classes_str))

        self.strand_classes = set(
            int(k)
            for k, v in CLASS_MAP["read_classification_map"].items()
            if v in self.pre_filter_classes
        )
        self.logger.debug("Strand-like classes are: {}.".format(self.strand_classes))
        self.logger.info("Creating rpc connection for device {}.".format(self.device))

        # try:
        #     from . import rpc
        #
        #     rpc._load()
        # except Exception as e:
        #     self.logger.warning("RPC module not found\n{}".format(e))
        #     self.logger.info("Attempting to load RPC")

        self.connection, self.message_port = get_rpc_connection(
            target_device=self.device,
            host=self.mk_host,
            port=self.mk_port,
            reload=self.reload_rpc,
        )

        self.logger.info("Loaded RPC")
        self.msgs = self.connection.data._pb

        log_waiting = True
        while parse_message(self.connection.acquisition.current_status())["status"] != "PROCESSING":
            if log_waiting:
                self.logger.info("Waiting for device to start processing")
                log_waiting = False

        self.mk_run_dir = Path(parse_message(
            self.connection.protocol.get_current_protocol_run()
        )["output_path"])

        # Create the output dir if it doesn't already exist
        # Sometimes we are faster than MinKNOW, this isn't a problem on OS X
        self.mk_run_dir.mkdir(parents=True, exist_ok=True)
        self.unblock_logger = setup_logger(
            # Necessary to use a str of the Path for 3.5 compatibility
            "unblocks",
            log_file=str(self.mk_run_dir / "unblocked_read_ids.txt"),
        )

        # Get signal calibrations
        self.calibration, self.calibration_dtype = {
            True: (self.msgs.GetLiveReadsRequest.CALIBRATED, "calibrated_signal",),
            False: (self.msgs.GetLiveReadsRequest.UNCALIBRATED, "uncalibrated_signal",),
        }.get(signal_calibration)

        _data_types = parse_message(self.connection.data.get_data_types())[
            self.calibration_dtype
        ]

        _signal_dtype = {
            "FLOATING_POINT": {2: "float16", 4: "float32"},
            "SIGNED_INTEGER": {2: "int16", 4: "int32"},
            "UNSIGNED_INTEGER": {2: "uint16", 4: "uint32"},
        }.get(_data_types["type"], {}).get(_data_types["size"], None)

        if _signal_dtype is not None:
            self.signal_dtype = np.dtype(_signal_dtype)
        else:
            raise NotImplementedError("Unrecognized signal dtype")

        self.logger.info("Signal data-type: {}".format(self.signal_dtype))
        # setup the queues and running status
        self._process_thread = None
        self.reset()

    def run(self, **kwargs):
        """Start the ReadUntilClient to get data from gRPC stream

        Other Parameters
        ----------------
        first_channel : int
        last_channel : int
        raw_data_type : np.dtype
        sample_minimum_chunk_size : int
        """
        self._process_thread = Thread(
            target=self._run, name=new_thread_name(), kwargs=kwargs
        )
        self._process_thread.start()
        self.logger.info("Processing started")

    def reset(self, timeout=5):
        """Reset the state of the client to an initial (not running)
        state with no data or requests in queues.

        """
        # self._process_reads is blocking => it runs in a thread.
        if self._process_thread is not None:
            self.logger.info("Reset request received, shutting down...")
            self.running.clear()
            # block, try hard for .cancel() on stream
            self._process_thread.join()
            if self._process_thread.is_alive():
                self.logger.warning("Stream handler did not finish correctly.")
            else:
                self.logger.info("Stream handler exited successfully.")
        self._process_thread = None

        # a flag to indicate whether gRPC stream is being processed. Any
        #  running ._runner() will respond to this.
        self.running = Event()
        # the action_queue is used to store unblock/stop_receiving_data
        #  requests before they are put on the gRPC stream.
        self.action_queue = queue.Queue()
        # the data_queue is used to store the latest chunk per channel
        self.data_queue = self.CacheType(size=self.cache_size)
        # stores all sent action ids -> unblock/stop
        self.sent_actions = dict()

    @property
    def acquisition_progress(self):
        """Get MinKNOW data acquisition progress.

        Returns
        -------
        An object with attributes .acquired and .processed

        """
        return self.connection.acquisition.get_progress().raw_per_channel

    @property
    def queue_length(self):
        """The length of the read queue."""
        return len(self.data_queue)

    @property
    def missed_reads(self):
        """Number of reads ejected from queue (i.e reads had one or more chunks
        enter into the analysis queue but were replaced with a distinct read
        before being pulled from the queue."""
        return self.data_queue.missed

    @property
    def missed_chunks(self):
        """Number of read chunks replaced in queue by a chunk from the same
        read (a single read may have its queued chunk replaced more than once).

        """
        return self.data_queue.replaced

    @property
    def is_running(self):
        """The processing status of the gRPC stream."""
        return self.running.is_set()

    def get_read_chunks(self, batch_size=1, last=True):
        """Get read chunks, removing them from the queue.

        Parameters
        ----------
        batch_size : int
            The maximum number of reads to retrieve from the ReadCache
        last : bool
            If True return most recently added reads, otherwise oldest

        Returns
        -------
        list
            List of tuples as (channel_number, raw_read)
        """
        return self.data_queue.popitems(n=batch_size, last=last)

    def unblock_read(self, read_channel, read_number, duration=0.1, read_id=None):
        """Request that a read be unblocked.

        Parameters
        ----------
        read_channel : int
            The channel number for the read to be unblocked
        read_number : int
            The read number for the read to be unblocked
        duration : float
            The time, in seconds, to apply the unblock voltage
        read_id : str
            The read id (UUID4) of the read to be unblocked

        Returns
        -------
        None
        """
        if read_id is not None:
            self.unblock_logger.debug(read_id)
        self._put_action(read_channel, read_number, "unblock", duration=duration)

    def stop_receiving_read(self, read_channel, read_number):
        """Request to receive no more data for a read.

        Parameters
        ----------
        read_channel : int
            The channel number for the read to be unblocked
        read_number : int
            The read number for the read to be unblocked

        Returns
        -------
        None
        """
        self._put_action(read_channel, read_number, "stop_further_data")

    def _run(self, **kwargs):
        """Manage conversion and processing of read chunks"""
        self.running.set()
        # .get_live_reads() takes an iterable of requests and generates
        #  raw data chunks and responses to our requests: the iterable
        #  thereby controls the lifetime of the stream. ._runner() as
        #  implemented below initialises the stream then transfers
        #  action requests from the action_queue to the stream.
        reads = self.connection.data.get_live_reads(self._runner(**kwargs))

        # ._process_reads() as implemented below is responsible for
        #  placing action requests on the queue and logging the responses.
        #  We really want to be calling reads.cancel() below so catch
        #  everything and anything.
        try:
            self._process_reads(reads)
        except Exception as e:
            self.logger.info(e)
            self.logger.info("MinKNOW may have finished acquisition, press Ctrl + C to exit ")
            # TODO: Catch the RPC error here to handle nicely?

        # signal to the server that we are done with the stream.
        reads.cancel()

    def _runner(
        self,
        first_channel=1,
        last_channel=512,
        min_chunk_size=ALLOWED_MIN_CHUNK_SIZE,
        action_batch=1000,
        action_throttle=0.1,
    ):
        """Yield the stream initializer request followed by action requests
        placed into the action_queue.

        Parameters
        ----------
        first_channel : int
            Lowest channel for which to receive raw data.
        last_channel : int
            Highest channel (inclusive) for which to receive data.
        min_chunk_size : int
            Minimum number of raw samples in a raw data chunk.
        action_batch : int
            Maximum number of actions to batch in a single response.
        action_throttle : float
            Time in seconds to wait between sending action batches

        Yields
        ------
        read_chunks
            From rpc.connection.data._pb.GetLiveReadRequest.StreamSetup
        """
        # see note at top of this module
        if min_chunk_size > ALLOWED_MIN_CHUNK_SIZE:
            self.logger.warning(
                "Reducing min_chunk_size to {}".format(ALLOWED_MIN_CHUNK_SIZE)
            )
            min_chunk_size = ALLOWED_MIN_CHUNK_SIZE

        self.logger.info(
            "Sending init command, channels:{}-{}, min_chunk:{}".format(
                first_channel, last_channel, min_chunk_size
            )
        )
        yield self.msgs.GetLiveReadsRequest(
            setup=self.msgs.GetLiveReadsRequest.StreamSetup(
                first_channel=first_channel,
                last_channel=last_channel,
                raw_data_type=self.calibration,
                sample_minimum_chunk_size=min_chunk_size,
            )
        )

        while self.is_running:
            t0 = time.time()
            # get as many items as we can up to the maximum, without blocking
            actions = list()
            for _ in range(action_batch):
                try:
                    action = self.action_queue.get_nowait()
                except queue.Empty:
                    break
                else:
                    actions.append(action)

            n_actions = len(actions)
            if n_actions > 0:
                self.logger.debug("Sending {} actions.".format(n_actions))
                action_group = self.msgs.GetLiveReadsRequest(
                    actions=self.msgs.GetLiveReadsRequest.Actions(actions=actions)
                )
                yield action_group

            # limit response interval
            t1 = time.time()
            if t0 + action_throttle > t1:
                time.sleep(action_throttle + t0 - t1)
        else:
            self.logger.info("Reset signal received by action handler.")

    def _process_reads(self, reads):
        """Process the gRPC stream data, storing read chunks in the data_queue.

        Parameters
        ----------
        reads : iterable
            Iterable of gRPC data stream as produced by get_live_reads()

        Returns
        -------
        None
        """
        response_counter = defaultdict(Counter)

        unique_reads = set()

        read_count = 0
        samples_behind = 0
        raw_data_bytes = 0
        for reads_chunk in reads:
            if not self.is_running:
                self.logger.info("Stopping processing of reads due to reset.")
                break
            # In each iteration, we get:
            #   i) responses to our previous actions (success/fail)
            #  ii) raw data for current reads

            # record a count of success and fails
            if len(reads_chunk.action_reponses):
                for response in reads_chunk.action_reponses:
                    action_type = self.sent_actions[response.action_id]
                    response_counter[action_type][response.response] += 1

            progress = self.acquisition_progress
            for read_channel in reads_chunk.channels:
                read_count += 1
                read = reads_chunk.channels[read_channel]
                if self.one_chunk:
                    if read.id in unique_reads:
                        # previous stop request wasn't enacted in time, don't
                        #  put the read back in the queue to avoid situation
                        #  where read has been popped from queue already and
                        #  we reinsert.
                        self.logger.debug(
                            "Rereceived {}:{} after stop request.".format(
                                read_channel, read.number
                            )
                        )
                        continue
                    self.stop_receiving_read(read_channel, read.number)
                unique_reads.add(read.id)
                read_samples_behind = progress.acquired - read.chunk_start_sample
                samples_behind += read_samples_behind
                raw_data_bytes += len(read.raw_data)

                # Removed list constructor around generator statement
                strand_like = any(
                    x in self.strand_classes for x in read.chunk_classifications
                )
                if not self.filter_strands or strand_like:
                    self.data_queue[read_channel] = read

    def _put_action(self, read_channel, read_number, action, **params):
        """Stores an action requests on the queue ready to be placed on the
        gRPC stream.

        Parameters
        ----------
        read_channel : int
            A read's channel number
        read_number : int
            A read's read number (the nth read per channel)
        action : str
            One of either 'stop_further_data' or 'unblock'

        Other Parameters
        ----------------
        duration : float
            Allowed when action is 'unblock'

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If action is not one of 'stop_further_data' or 'unblock'
        """
        action_id = str(uuid.uuid4())
        action_kwargs = {
            "action_id": action_id,
            "channel": read_channel,
            "number": read_number,
        }
        self.sent_actions[action_id] = action
        if action == "stop_further_data":
            action_kwargs[action] = self.msgs.GetLiveReadsRequest.StopFurtherData()
        elif action == "unblock":
            action_kwargs[action] = self.msgs.GetLiveReadsRequest.UnblockAction()
            if "duration" in params:
                action_kwargs[action].duration = params["duration"]
        else:
            raise ValueError("action parameter must be stop_further_data or unblock")

        action_request = self.msgs.GetLiveReadsRequest.Action(**action_kwargs)
        self.action_queue.put(action_request)
