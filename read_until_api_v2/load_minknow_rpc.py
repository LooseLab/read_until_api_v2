"""load_minknow_rpc.py

This is how we get around having to use python2 and install inside the MinKNOW
directory. We simply:
 - Get the default MinKNOW installation directory
 - Gently reach in and rip out its beating heart (the minknow.rpc module)
 - Make a copy here, substituting import statements on the fly to allow imports
 - Patch the __init__.py to account for finding certificates
"""
import fileinput
import fnmatch
import os
import platform
import shutil
import sys

from google.protobuf.json_format import MessageToDict

PATCH_INIT = """
import platform
import os
def _minknow_path(operating_system=platform.system()):
    return {
        "Darwin": os.path.join(os.sep, "Applications", "MinKNOW.app", "Contents", "Resources"),
        "Linux": os.path.join(os.sep, "opt", "ONT", "MinKNOW"),
        "Windows": os.path.join(os.sep, "C:\\\Program Files", "OxfordNanopore", "MinKNOW"),
    }.get(operating_system, None)
"""

READ_UNTIL_DIR = os.path.dirname(os.path.realpath(__file__))
OPER = platform.system()
MK_PATH = os.path.join("ont-python", "lib", "python2.7", "site-packages", "minknow")


def _minknow_path(operating_system=OPER):
    """Return default MinKNOW path."""
    return {
        "Darwin": os.path.join(
            os.sep, "Applications", "MinKNOW.app", "Contents", "Resources"
        ),
        "Linux": os.path.join(os.sep, "opt", "ONT", "MinKNOW"),
        "Windows": os.path.join(
            os.sep, "C:\\\Program Files", "OxfordNanopore", "MinKNOW"
        ),
    }.get(operating_system, None)


def _mk_module_path(module, operating_system=OPER):
    """Return MinKNOW site-package dir relative to ont-python dir."""
    mk_m_path = {
        "Windows": os.path.join("ont-python", "Lib", "site-packages", "minknow")
    }.get(operating_system, MK_PATH)
    return os.path.join(mk_m_path, module)


def copy_files(
    source_dir,
    destination_dir,
    file_pattern,
    target_module,
    current_package=vars(sys.modules[__name__])["__package__"],
):
    """Copy a module from another location

    Parameters
    ----------
    source_dir : str
        Source directory to copy from
    destination_dir : str
        Destination directory, where files are copied to
    file_pattern : str
        File pattern to copy, eg '*.py'
    target_module : str
        Module to copy from the source_dir
    current_package : str
        Current package, this is automatically detected, but may not work
        for nested modules

    Returns
    -------
    None
    """

    def failed(exc):
        raise exc

    destination_dir = os.path.join(READ_UNTIL_DIR, destination_dir)
    for dir_path, dirs, files in os.walk(source_dir, topdown=True, onerror=failed):
        for file in fnmatch.filter(files, file_pattern):
            shutil.copy2(os.path.join(dir_path, file), destination_dir)
            edit_file(
                os.path.join(destination_dir, file),
                "minknow.{}".format(target_module),
                "{}.{}".format(current_package, target_module),
            )
            edit_file(
                os.path.join(destination_dir, file),
                "minknow.paths.minknow_base_dir()",
                "_minknow_path()",
            )
            edit_file(os.path.join(destination_dir, file), "import minknow.paths", "")
            if file == "__init__.py":
                with open(os.path.join(destination_dir, file), "a") as out:
                    out.write(PATCH_INIT)

        break  # no recursion


def edit_file(filename, text_to_search, replacement_text):
    """Edit file inplace replacing matching text."""
    with fileinput.FileInput(filename, inplace=True) as file:
        for line in file:
            print(line.replace(text_to_search, replacement_text), end="")


def load_rpc(always_reload=False):
    """Load the minknow.rpc module"""
    destination_rpc = ("rpc",)

    for module in destination_rpc:
        # Remove current RPC
        if always_reload and os.path.exists(os.path.join(READ_UNTIL_DIR, module)):
            shutil.rmtree(os.path.join(READ_UNTIL_DIR, module))

        # Make new RPC directory
        if not os.path.exists(os.path.join(READ_UNTIL_DIR, module)):
            os.makedirs(os.path.join(READ_UNTIL_DIR, module))

        # Copy files from MinKNOW
        if not os.path.isfile(os.path.join(READ_UNTIL_DIR, module, "__init__.py")):
            if _minknow_path() is not None:
                source_rpc = os.path.join(_minknow_path(), _mk_module_path(module))
            else:
                raise NotImplementedError("MinKNOW not found on this platform")

            if os.path.exists(source_rpc):
                copy_files(source_rpc, module, "*.py", module)
            else:
                raise ValueError("MinKNOW not found on this computer")

        sys.path.insert(0, os.path.join(READ_UNTIL_DIR, module))


def get_rpc_connection(target_device, host="127.0.0.1", port=9501, reload=False):
    """Return gRPC connection and message port for a target device

    Parameters
    ----------
    target_device : str
        ...
    host : str
        ...
    port : int
        ...
    reload : bool
        ...

    Returns
    -------
    connection
    message_port
    """
    load_rpc(reload)
    import grpc
    from . import rpc

    rpc._load()
    from .rpc import manager_pb2 as manager
    from .rpc import manager_pb2_grpc as manager_grpc

    channel = grpc.insecure_channel("{}:{}".format(host, port))
    stub = manager_grpc.ManagerServiceStub(channel)
    list_request = manager.ListDevicesRequest()
    response = stub.list_devices(list_request)
    for device in response.active:
        if device.name == target_device:
            rpc_connection = rpc.Connection(port=device.ports.insecure_grpc)
            message_port = device.ports.jsonrpc
            return rpc_connection, message_port

    raise ValueError("Device not recognised.")


def parse_message(message):
    """Parse gRPC message to dict."""
    return MessageToDict(
        message, preserving_proto_field_name=True, including_default_value_fields=True,
    )
