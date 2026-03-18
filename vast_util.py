from __future__ import unicode_literals, print_function, annotations

##################################################################################################
# Imports
##################################################################################################
import re
import json
import sys
import argparse
import os
import time
from typing import Dict, List, Tuple, Optional
from datetime import date, datetime, timedelta, timezone
import hashlib
import math
import threading
from concurrent.futures import ThreadPoolExecutor
import requests
import getpass
import subprocess
from time import sleep
from subprocess import PIPE
import urllib3
import atexit
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from typing import Optional
import shutil
import logging
import textwrap
from pathlib import Path
import warnings
import importlib.metadata


from copy import deepcopy

PYPI_BASE_PATH = "https://pypi.org"
# INFO - Change to False if you don't want to check for update each run.
should_check_for_update = False
ARGS = None
TABCOMPLETE = False
try:
    import argcomplete
    TABCOMPLETE = True
except:
    # No tab-completion for you
    pass

try:
    import curlify
except ImportError:
    pass

try:
    from urllib import quote_plus  # Python 2.X
except ImportError:
    from urllib.parse import quote_plus  # Python 3+

try:
    JSONDecodeError = json.JSONDecodeError
except AttributeError:
    JSONDecodeError = ValueError


##################################################################################################
# Configuration & Environment
##################################################################################################

#server_url_default = "https://vast.ai"
server_url_default = os.getenv("VAST_URL") or "https://console.vast.ai"
#server_url_default = "http://localhost:5002"
#server_url_default = "host.docker.internal"
#server_url_default = "http://localhost:5002"
#server_url_default  = "https://vast.ai/api/v0"

logging.basicConfig(
    level=os.getenv("LOGLEVEL") or logging.WARN,
    format="%(levelname)s - %(message)s"
)


##################################################################################################
# Version Management
##################################################################################################
def parse_version(version: str) -> tuple[int, ...]:
    parts = version.split(".")

    if len(parts) < 3:
        print(f"Invalid version format: {version}", file=sys.stderr)

    return tuple(int(part) for part in parts)


def get_git_version():
    try:
        result = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0"],
            capture_output=True,
            text=True,
            check=True,
        )
        tag = result.stdout.strip()

        return tag[1:] if tag.startswith("v") else tag
    except Exception:
        return "0.0.0"


def get_pip_version():
    try:
        return importlib.metadata.version("vastai")
    except Exception:
        return "0.0.0"


def is_pip_package():
    try:
        return importlib.metadata.metadata("vastai") is not None
    except Exception:
        return False


def get_update_command(stable_version: str) -> str:
    if is_pip_package():
        if "test.pypi.org" in PYPI_BASE_PATH:
            return f"{sys.executable} -m pip install --force-reinstall --no-cache-dir -i {PYPI_BASE_PATH} vastai=={stable_version}"
        else:
            return f"{sys.executable} -m pip install --force-reinstall --no-cache-dir vastai=={stable_version}"
    else:
        return f"git fetch --all --tags --prune && git checkout tags/v{stable_version}"


def get_local_version():
    if is_pip_package():
        return get_pip_version()
    return get_git_version()


def get_project_data(project_name: str) -> dict[str, dict[str, str]]:
    url = PYPI_BASE_PATH + f"/pypi/{project_name}/json"
    response = requests.get(url, headers={"Accept": "application/json"})

    # this will raise for HTTP status 4xx and 5xx
    response.raise_for_status()

    # this will raise for HTTP status >200,<=399
    if response.status_code != 200:
        raise Exception(
            f"Could not get PyPi Project: {project_name}. Response: {response.status_code}"
        )

    response_data: dict[str, dict[str, str]] = response.json()
    return response_data


def get_pypi_version(project_data: dict[str, dict[str, str]]) -> str:
    info_data = project_data.get("info")

    if not info_data:
        raise Exception("Could not get PyPi Project")

    version_data: str = str(info_data.get("version"))

    return str(version_data)


def check_for_update():
    pypi_data = get_project_data("vastai")
    pypi_version = get_pypi_version(pypi_data)

    local_version = get_local_version()

    local_tuple = parse_version(local_version)
    pypi_tuple = parse_version(pypi_version)

    if local_tuple >= pypi_tuple:
        return

    user_wants_update = input(
        f"Update available from {local_version} to {pypi_version}. Would you like to update [Y/n]: "
    ).lower()

    if user_wants_update not in ["y", ""]:
        print("You selected no. If you don't want to check for updates each time, update should_check_for_update in vast.py")
        return

    update_command = get_update_command(pypi_version)

    print("Updating...")
    _ = subprocess.run(
        update_command,
        shell=True,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    print("Update completed successfully!\nAttempt to run your command again!")
    sys.exit(0)


##################################################################################################
# Constants
##################################################################################################
APP_NAME = "vastai"
VERSION = get_local_version()

# define emoji support and fallbacks
_HAS_EMOJI = sys.stdout.encoding and 'utf' in sys.stdout.encoding.lower()
SUCCESS = "✅" if _HAS_EMOJI else "[OK]"
WARN    = "⚠️" if _HAS_EMOJI else "[!]"
FAIL    = "❌" if _HAS_EMOJI else "[X]"
INFO    = "ℹ️" if _HAS_EMOJI else "[i]"

try:
  # Although xdg-base-dirs is the newer name, there's 
  # python compatibility issues with dependencies that
  # can be unresolvable using things like python 3.9
  # So we actually use the older name, thus older
  # version for now. This is as of now (2024/11/15)
  # the safer option. -cjm
  import xdg

  DIRS = {
      'config': xdg.xdg_config_home(),
      'temp': xdg.xdg_cache_home()
  }

except:
  # Reasonable defaults.
  DIRS = {
      'config': os.path.join(os.getenv('HOME'), '.config'),
      'temp': os.path.join(os.getenv('HOME'), '.cache'),
  }

for key in DIRS.keys():
  DIRS[key] = path = os.path.join(DIRS[key], APP_NAME)
  if not os.path.exists(path):
    os.makedirs(path)

CACHE_FILE = os.path.join(DIRS['temp'], "gpu_names_cache.json")
CACHE_DURATION = timedelta(hours=24)

APIKEY_FILE = os.path.join(DIRS['config'], "vast_api_key")
APIKEY_FILE_HOME = os.path.expanduser("~/.vast_api_key") # Legacy
TFAKEY_FILE = os.path.join(DIRS['config'], "vast_tfa_key")

if not os.path.exists(APIKEY_FILE) and os.path.exists(APIKEY_FILE_HOME):
  #print(f'copying key from {APIKEY_FILE_HOME} -> {APIKEY_FILE}')
  shutil.copyfile(APIKEY_FILE_HOME, APIKEY_FILE)


api_key_guard = object()
headers = {}


##################################################################################################
# Classes
##################################################################################################
class Object(object):
    pass


##################################################################################################
# Input Validation
##################################################################################################


def validate_seconds(value):
    """Validate that the input value is a valid number for seconds between yesterday and Jan 1, 2100."""
    try:
        val = int(value)
        
        # Calculate min_seconds as the start of yesterday in seconds
        yesterday = datetime.now() - timedelta(days=1)
        min_seconds = int(yesterday.timestamp())
        
        # Calculate max_seconds for Jan 1st, 2100 in seconds
        max_date = datetime(2100, 1, 1, 0, 0, 0)
        max_seconds = int(max_date.timestamp())
        
        if not (min_seconds <= val <= max_seconds):
            raise argparse.ArgumentTypeError(f"{value} is not a valid second timestamp.")
        return val
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value} is not a valid integer.")


def strip_strings(value):
    if isinstance(value, str):
        return value.strip()
    elif isinstance(value, dict):
        return {k: strip_strings(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [strip_strings(item) for item in value]
    return value  # Return as is if not a string, list, or dict


def string_to_unix_epoch(date_string):
    if date_string is None:
        return None
    try:
        # Check if the input is a float or integer representing Unix time
        return float(date_string)
    except ValueError:
        # If not, parse it as a date string
        date_object = datetime.strptime(date_string, "%m/%d/%Y")
        return time.mktime(date_object.timetuple())


def unix_to_readable(ts):
    # ts: integer or float, Unix timestamp
    return datetime.fromtimestamp(ts).strftime('%H:%M:%S|%h-%d-%Y')


def fix_date_fields(query: Dict[str, Dict], date_fields: List[str]):
    """Takes in a query and date fields to correct and returns query with appropriate epoch dates"""
    new_query: Dict[str, Dict] = {}
    for field, sub_query in query.items():
        # fix date values for given date fields
        if field in date_fields:
            new_sub_query = {k: string_to_unix_epoch(v) for k, v in sub_query.items()}
            new_query[field] = new_sub_query
        # else, use the original
        else: new_query[field] = sub_query

    return new_query


##################################################################################################
# CLI Infrastructure
##################################################################################################
class argument(object):
    def __init__(self, *args, mutex_group=None, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.mutex_group = mutex_group  # Name of the mutually exclusive group this arg belongs to


class hidden_aliases(object):
    # just a bit of a hack
    def __init__(self, l):
        self.l = l

    def __iter__(self):
        return iter(self.l)

    def __bool__(self):
        return False

    def __nonzero__(self):
        return False

    def append(self, x):
        self.l.append(x)


def load_permissions_from_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)


def complete_instance_machine(prefix=None, action=None, parser=None, parsed_args=None):
    try:
        from . import vast as _vast
    except ImportError:
        import vast as _vast  # type: ignore
    return _vast.show__instances(ARGS, {'internal': True, 'field': 'machine_id'})


def complete_instance(prefix=None, action=None, parser=None, parsed_args=None):
    try:
        from . import vast as _vast
    except ImportError:
        import vast as _vast  # type: ignore
    return _vast.show__instances(ARGS, {'internal': True, 'field': 'id'})


def complete_sshkeys(prefix=None, action=None, parser=None, parsed_args=None):
  return [str(m) for m in Path.home().joinpath('.ssh').glob('*.pub')]


class apwrap(object):
    def __init__(self, *args, **kwargs):
        if "formatter_class" not in kwargs:
            kwargs["formatter_class"] = MyWideHelpFormatter    
        self.parser = argparse.ArgumentParser(*args, **kwargs)
        self.parser.set_defaults(func=self.fail_with_help)
        self.subparsers_ = None
        self.subparser_objs = []
        self.added_help_cmd = False
        self.post_setup = []
        self.verbs = set()
        self.objs = set()

    def fail_with_help(self, *a, **kw):
        self.parser.print_help(sys.stderr)
        raise SystemExit

    def add_argument(self, *a, **kw):
        if not kw.get("parent_only"):
            for x in self.subparser_objs:
                try:
                    # Create a global options group for better visual separation
                    if not hasattr(x, '_global_options_group'):
                        x._global_options_group = x.add_argument_group('Global options (available for all commands)')
                    # Use SUPPRESS as default for subparsers so they don't overwrite
                    # values already set by the main parser when the argument is placed
                    # before the subcommand (e.g., `vastai --url <url> get wrkgrp-logs`)
                    subparser_kw = kw.copy()
                    subparser_kw['default'] = argparse.SUPPRESS
                    x._global_options_group.add_argument(*a, **subparser_kw)
                except argparse.ArgumentError:
                    # duplicate - or maybe other things, hopefully not
                    pass
        return self.parser.add_argument(*a, **kw)

    def subparsers(self, *a, **kw):
        if self.subparsers_ is None:
            kw["metavar"] = "command"
            kw["help"] = "command to run. one of:"
            self.subparsers_ = self.parser.add_subparsers(*a, **kw)
        return self.subparsers_

    def get_name(self, verb, obj):
        if obj:
            self.verbs.add(verb)
            self.objs.add(obj)
            name = verb + ' ' + obj
        else:
            self.objs.add(verb)
            name = verb
        return name

    def command(self, *arguments, aliases=(), help=None, **kwargs):
        help_ = help
        if not self.added_help_cmd:
            self.added_help_cmd = True

            @self.command(argument("subcommand", default=None, nargs="?"), help="print this help message")
            def help(*a, **kw):
                self.fail_with_help()

        def inner(func):
            dashed_name = func.__name__.replace("_", "-")
            verb, _, obj = dashed_name.partition("--")
            name = self.get_name(verb, obj)
            aliases_transformed = [] if aliases else hidden_aliases([])
            for x in aliases:
                verb, _, obj = x.partition(" ")
                aliases_transformed.append(self.get_name(verb, obj))
            if "formatter_class" not in kwargs:
                kwargs["formatter_class"] = MyWideHelpFormatter

            sp = self.subparsers().add_parser(name, aliases=aliases_transformed, help=help_, **kwargs)

            # TODO: Sometimes the parser.command has a help parameter. Ideally
            # I'd extract this during the sdk phase but for the life of me
            # I can't find it.
            setattr(func, "mysignature", sp)
            setattr(func, "mysignature_help", help_)

            self.subparser_objs.append(sp)
            
            self._process_arguments_with_groups(sp, arguments)

            sp.set_defaults(func=func)
            return func

        if len(arguments) == 1 and type(arguments[0]) != argument:
            func = arguments[0]
            arguments = []
            return inner(func)
        return inner

    def parse_args(self, argv=None, *a, **kw):
        if argv is None:
            argv = sys.argv[1:]
        argv_ = []
        for x in argv:
            if argv_ and argv_[-1] in self.verbs:
                argv_[-1] += " " + x
            else:
                argv_.append(x)
        args = self.parser.parse_args(argv_, *a, **kw)
        for func in self.post_setup:
            func(args)
        return args

    def _process_arguments_with_groups(self, parser_obj, arguments):
        """Process arguments and handle mutually exclusive groups"""
        mutex_groups_to_required = {}
        arg_to_group = {}
        
        # Determine if any mutex groups are required
        for arg in arguments:
            key = arg.args[0]
            if arg.mutex_group:
                is_required = arg.kwargs.pop('required', False)
                group_name = arg.mutex_group
                arg_to_group[key] = group_name
                if mutex_groups_to_required.get(group_name):
                    continue  # if marked as required then it stays required
                else:
                    mutex_groups_to_required[group_name] = is_required
        
        name_to_group_parser = {}  # Create mutually exclusive group parsers
        for group_name, is_required in mutex_groups_to_required.items():
            mutex_group = parser_obj.add_mutually_exclusive_group(required=is_required)
            name_to_group_parser[group_name] = mutex_group

        for arg in arguments:  # Add args via the appropriate parser
            key = arg.args[0]
            if arg_to_group.get(key):
                group_parser = name_to_group_parser[arg_to_group[key]]
                tsp = group_parser.add_argument(*arg.args, **arg.kwargs)
            else:
                tsp = parser_obj.add_argument(*arg.args, **arg.kwargs)
            self._add_completer(tsp, arg)
            

    def _add_completer(self, tsp, arg):
        """Helper function to add completers based on argument names"""
        myCompleter = None
        comparator = arg.args[0].lower()
        if comparator.startswith('machine'):
            myCompleter = complete_instance_machine
        elif comparator.startswith('id') or comparator.endswith('id'):
            myCompleter = complete_instance
        elif comparator.startswith('ssh'):
            myCompleter = complete_sshkeys
            
        if myCompleter:
            setattr(tsp, 'completer', myCompleter)


class MyWideHelpFormatter(argparse.RawTextHelpFormatter):
    def __init__(self, prog):
        super().__init__(prog, width=128, max_help_position=50, indent_increment=1)


parser = apwrap(
    epilog="Use 'vast COMMAND --help' for more info about a command",
    formatter_class=MyWideHelpFormatter
)


##################################################################################################
# HTTP Utilities
##################################################################################################
def http_request(verb, args, req_url, headers: dict[str, str] | None = None, json_data = None):
    t = 0.15
    for i in range(0, args.retry):
        req = requests.Request(method=verb, url=req_url, headers=headers, json=json_data)
        session = requests.Session()
        prep = session.prepare_request(req)
        if args.explain:
            print(f"\n{INFO}  Prepared Request:")
            print(f"{prep.method} {prep.url}")
            print(f"Headers: {json.dumps(headers, indent=1)}")
            print(f"Body: {json.dumps(json_data, indent=1)}" + "\n" + "_"*100 + "\n")
        
        if ARGS.curl:
            as_curl = curlify.to_curl(prep)
            simple = re.sub(r" -H '[^']*'", '', as_curl)
            parts = re.split(r'(?=\s+-\S+)', simple)
            pp = parts[-1].split("'")
            pp[-3] += "\n "
            parts = [*parts[:-1], *[x.rstrip() for x in "'".join(pp).split("\n")]]
            print("\n" + ' \\\n  '.join(parts).strip() + "\n")
            sys.exit(0)
        else:
            r = session.send(prep)

        if (r.status_code == 429):
            time.sleep(t)
            t *= 1.5
        else:
            break
    return r


def http_get(args, req_url, headers = None, json = None):
    return http_request('GET', args, req_url, headers, json)


def http_put(args, req_url, headers = None, json = {}):
    return http_request('PUT', args, req_url, headers, json)


def http_post(args, req_url, headers = None, json={}):
    return http_request('POST', args, req_url, headers, json)


def http_del(args, req_url, headers = None, json={}):
    return http_request('DELETE', args, req_url, headers, json)


def apiurl(args: argparse.Namespace, subpath: str, query_args: Dict = None) -> str:
    """Creates the endpoint URL for a given combination of parameters.

    :param argparse.Namespace args: Namespace with many fields relevant to the endpoint.
    :param str subpath: added to end of URL to further specify endpoint.
    :param typing.Dict query_args: specifics such as API key and search parameters that complete the URL.
    :rtype str:
    """
    result = None

    if query_args is None:
        query_args = {}
    if args.api_key is not None:
        query_args["api_key"] = args.api_key
    if not re.match(r"^/api/v(\d)+/", subpath):
        subpath = "/api/v0" + subpath
    
    query_json = None

    if query_args:
        # a_list      = [<expression> for <l-expression> in <expression>]
        '''
        vector result
        for (l_expression: expression) {
            result.push_back(expression)
        }
        '''
        # an_iterator = (<expression> for <l-expression> in <expression>)

        query_json = "&".join(
            "{x}={y}".format(x=x, y=quote_plus(y if isinstance(y, str) else json.dumps(y))) for x, y in
            query_args.items())
        
        result = args.url + subpath + "?" + query_json
    else:
        result = args.url + subpath

    if (args.explain):
        print("query args:")
        print(query_args)
        print("")
        print(f"base: {args.url + subpath + '?'} + query: ")
        print(result)
        print("")
    return result


def apiheaders(args: argparse.Namespace) -> Dict:
    """Creates the headers for a given combination of parameters.

    :param argparse.Namespace args: Namespace with many fields relevant to the endpoint.
    :rtype Dict:
    """
    result = {}
    if args.api_key is not None:
        result["Authorization"] = "Bearer " + args.api_key
    return result 


def deindent(message: str, add_separator: bool = True) -> str:
    """
    Deindent a quoted string. Scans message and finds the smallest number of whitespace characters in any line and
    removes that many from the start of every line.

    :param str message: Message to deindent.
    :rtype str:
    """
    message = re.sub(r" *$", "", message, flags=re.MULTILINE)
    indents = [len(x) for x in re.findall("^ *(?=[^ ])", message, re.MULTILINE) if len(x)]
    a = min(indents)
    message = re.sub(r"^ {," + str(a) + "}", "", message, flags=re.MULTILINE)
    if add_separator:
        # For help epilogs - cleanly separating extra help from options
        line_width = min(150, shutil.get_terminal_size((80, 20)).columns)
        message = "_"*line_width + "\n"*2 + message.strip() + "\n" + "_"*line_width
    return message.strip()


def translate_null_strings_to_blanks(d: Dict) -> Dict:
    """Map over a dict and translate any null string values into ' '.
    Leave everything else as is. This is needed because you cannot add TableCell
    objects with only a null string or the client crashes.

    :param Dict d: dict of item values.
    :rtype Dict:
    """

    # Beware: locally defined function.
    def translate_nulls(s):
        if s == "":
            return " "
        return s

    new_d = {k: translate_nulls(v) for k, v in d.items()}
    return new_d

    #req_url = apiurl(args, "/instances", {"owner": "me"})


##################################################################################################
# Field Definitions
##################################################################################################
# These are the fields that are displayed when a search is run
displayable_fields = (
    # ("bw_nvlink", "Bandwidth NVLink", "{}", None, True),
    ("id", "ID", "{}", None, True),
    ("cuda_max_good", "CUDA", "{:0.1f}", None, True),
    ("num_gpus", "N", "{}x", None, False),
    ("gpu_name", "Model", "{}", None, True),
    ("pcie_bw", "PCIE", "{:0.1f}", None, True),
    ("cpu_ghz", "cpu_ghz", "{:0.1f}", None, True),
    ("cpu_cores_effective", "vCPUs", "{:0.1f}", None, True),
    ("cpu_ram", "RAM", "{:0.1f}", lambda x: x / 1000, False),
    ("gpu_ram", "VRAM", "{:0.1f}", lambda x: x / 1000, False),
    ("disk_space", "Disk", "{:.0f}", None, True),
    ("dph_total", "$/hr", "{:0.4f}", None, True),
    ("dlperf", "DLP", "{:0.1f}", None, True),
    ("dlperf_per_dphtotal", "DLP/$", "{:0.2f}", None, True),
    ("score", "score", "{:0.1f}", None, True),
    ("driver_version", "NV Driver", "{}", None, True),
    ("inet_up", "Net_up", "{:0.1f}", None, True),
    ("inet_down", "Net_down", "{:0.1f}", None, True),
    ("reliability", "R", "{:0.1f}", lambda x: x * 100, True),
    ("duration", "Max_Days", "{:0.1f}", lambda x: x / (24.0 * 60.0 * 60.0), True),
    ("machine_id", "mach_id", "{}", None, True),
    ("verification", "status", "{}", None, True),
    ("host_id", "host_id", "{}", None, True),
    ("direct_port_count", "ports", "{}", None, True),
    ("geolocation", "country", "{}", None, True),
   #  ("direct_port_count", "Direct Port Count", "{}", None, True),
)
displayable_fields_reserved = (
    # ("bw_nvlink", "Bandwidth NVLink", "{}", None, True),
    ("id", "ID", "{}", None, True),
    ("cuda_max_good", "CUDA", "{:0.1f}", None, True),
    ("num_gpus", "N", "{}x", None, False),
    ("gpu_name", "Model", "{}", None, True),
    ("pcie_bw", "PCIE", "{:0.1f}", None, True),
    ("cpu_ghz", "cpu_ghz", "{:0.1f}", None, True),
    ("cpu_cores_effective", "vCPUs", "{:0.1f}", None, True),
    ("cpu_ram", "RAM", "{:0.1f}", lambda x: x / 1000, False),
    ("disk_space", "Disk", "{:.0f}", None, True),
    ("discounted_dph_total", "$/hr", "{:0.4f}", None, True),
    ("dlperf", "DLP", "{:0.1f}", None, True),
    ("dlperf_per_dphtotal", "DLP/$", "{:0.2f}", None, True),
    ("driver_version", "NV Driver", "{}", None, True),
    ("inet_up", "Net_up", "{:0.1f}", None, True),
    ("inet_down", "Net_down", "{:0.1f}", None, True),
    ("reliability", "R", "{:0.1f}", lambda x: x * 100, True),
    ("duration", "Max_Days", "{:0.1f}", lambda x: x / (24.0 * 60.0 * 60.0), True),
    ("machine_id", "mach_id", "{}", None, True),
    ("verification", "status", "{}", None, True),
    ("host_id", "host_id", "{}", None, True),
    ("direct_port_count", "ports", "{}", None, True),
    ("geolocation", "country", "{}", None, True),
   #  ("direct_port_count", "Direct Port Count", "{}", None, True),
)
vol_offers_fields = {
        "cpu_arch",
        "cuda_vers",
        "cluster_id",
        "nw_disk_min_bw",
        "nw_disk_avg_bw",
        "nw_disk_max_bw",
        "datacenter",
        "disk_bw",
        "disk_space",
        "driver_version",
        "duration",
        "geolocation",
        "gpu_arch",
        "has_avx",
        "host_id",
        "id",
        "inet_down",
        "inet_up",
        "machine_id",
        "pci_gen",
        "pcie_bw",
        "reliability",
        "storage_cost",
        "static_ip",
        "total_flops",
        "ubuntu_version",
        "verified",
}
vol_displayable_fields = (
    ("id", "ID", "{}", None, True),
    ("cuda_max_good", "CUDA", "{:0.1f}", None, True),
    ("cpu_ghz", "cpu_ghz", "{:0.1f}", None, True),
    ("disk_bw", "Disk B/W", "{:0.1f}", None, True),
    ("disk_space", "Disk", "{:.0f}", None, True),
    ("disk_name", "Disk Name", "{}", None, True),
    ("storage_cost", "$/Gb/Month", "{:.2f}", None, True),
    ("driver_version", "NV Driver", "{}", None, True),
    ("inet_up", "Net_up", "{:0.1f}", None, True),
    ("inet_down", "Net_down", "{:0.1f}", None, True),
    ("reliability", "R", "{:0.1f}", lambda x: x * 100, True),
    ("duration", "Max_Days", "{:0.1f}", lambda x: x / (24.0 * 60.0 * 60.0), True),
    ("machine_id", "mach_id", "{}", None, True),
    ("verification", "status", "{}", None, True),
    ("host_id", "host_id", "{}", None, True),
    ("geolocation", "country", "{}", None, True),
)
nw_vol_displayable_fields = (
    ("id", "ID", "{}", None, True),
    ("disk_space", "Disk", "{:.0f}", None, True),
    ("storage_cost", "$/Gb/Month", "{:.2f}", None, True),
    ("inet_up", "Net_up", "{:0.1f}", None, True),
    ("inet_down", "Net_down", "{:0.1f}", None, True),
    ("reliability", "R", "{:0.1f}", lambda x: x * 100, True),
    ("duration", "Max_Days", "{:0.1f}", lambda x: x / (24.0 * 60.0 * 60.0), True),
    ("verification", "status", "{}", None, True),
    ("host_id", "host_id", "{}", None, True),
    ("cluster_id", "cluster_id", "{}", None, True),
    ("geolocation", "country", "{}", None, True),
    ("nw_disk_min_bw", "Min BW MiB/s", "{}", None, True),
    ("nw_disk_max_bw", "Max BW MiB/s", "{}", None, True),
    ("nw_disk_avg_bw", "Avg BW MiB/s", "{}", None, True),

)
# Need to add bw_nvlink, machine_id, direct_port_count to output.


# These fields are displayed when you do 'show instances'
instance_fields = (
    ("id", "ID", "{}", None, True),
    ("machine_id", "Machine", "{}", None, True),
    ("actual_status", "Status", "{}", None, True),
    ("num_gpus", "Num", "{}x", None, False),
    ("gpu_name", "Model", "{}", None, True),
    ("gpu_util", "Util. %", "{:0.1f}", None, True),
    ("cpu_cores_effective", "vCPUs", "{:0.1f}", None, True),
    ("cpu_ram", "RAM", "{:0.1f}", lambda x: x / 1000, False),
    ("disk_space", "Storage", "{:.0f}", None, True),
    ("ssh_host", "SSH Addr", "{}", None, True),
    ("ssh_port", "SSH Port", "{}", None, True),
    ("dph_total", "$/hr", "{:0.4f}", None, True),
    ("image_uuid", "Image", "{}", None, True),
    # ("dlperf",              "DLPerf",   "{:0.1f}",  None, True),
    # ("dlperf_per_dphtotal", "DLP/$",    "{:0.1f}",  None, True),
    ("inet_up", "Net up", "{:0.1f}", None, True),
    ("inet_down", "Net down", "{:0.1f}", None, True),
    ("reliability2", "R", "{:0.1f}", lambda x: x * 100, True),
    ("label", "Label", "{}", None, True),
    ("duration", "age(hours)", "{:0.2f}",  lambda x: x/(3600.0), True),
    ("uptime_mins", "uptime(mins)", "{:0.2f}",  None, True),
)


cluster_fields = (
    ("id", "ID", "{}", None, True),
    ("subnet", "Subnet", "{}", None, True),
    ("node_count", "Nodes", "{}", None, True),
    ("manager_id", "Manager ID", "{}", None, True),
    ("manager_ip", "Manager IP", "{}", None, True),
    ("machine_ids", "Machine ID's", "{}", None, True)
)

network_disk_fields = (
    ("network_disk_id", "Network Disk ID", "{}", None, True),
    ("free_space", "Free Space (GB)", "{}", None, True),
    ("total_space", "Total Space (GB)", "{}", None, True),
)
network_disk_machine_fields = (
    ("machine_id", "Machine ID", "{}", None, True),
    ("mount_point", "Mount Point", "{}", None, True),
)
overlay_fields = (
    ("overlay_id", "Overlay ID", "{}", None, True),
    ("name", "Name", "{}", None, True),
    ("subnet", "Subnet", "{}", None, True),
    ("cluster_id", "Cluster ID", "{}", None, True),
    ("instance_count", "Instances", "{}", None, True),
    ("instances", "Instance IDs", "{}", None, True),
)
volume_fields = (
    ("id", "ID", "{}", None, True),
    ("cluster_id", "Cluster ID", "{}", None, True),
    ("label", "Name", "{}", None, True),
    ("disk_space", "Disk", "{:.0f}", None, True),
    ("status", "status", "{}", None, True),
    ("disk_name", "Disk Name", "{}", None, True),
    ("driver_version", "NV Driver", "{}", None, True),
    ("inet_up", "Net_up", "{:0.1f}", None, True),
    ("inet_down", "Net_down", "{:0.1f}", None, True),
    ("reliability2", "R", "{:0.1f}", lambda x: x * 100, True),
    ("duration", "age(hours)", "{:0.2f}", lambda x: x/(3600.0), True),
    ("machine_id", "mach_id", "{}", None, True),
    ("verification", "Verification", "{}", None, True),
    ("host_id", "host_id", "{}", None, True),
    ("geolocation", "country", "{}", None, True),
    ("instances", "instances","{}", None, True)
)

# These fields are displayed when you do 'show machines'
machine_fields = (
    ("id", "ID", "{}", None, True),
    ("num_gpus", "#gpus", "{}", None, True),
    ("gpu_name", "gpu_name", "{}", None, True),
    ("disk_space", "disk", "{}", None, True),
    ("hostname", "hostname", "{}", lambda x: x[:16], True),
    ("driver_version", "driver", "{}", None, True),
    ("reliability2", "reliab", "{:0.4f}", None, True),
    ("verification", "veri", "{}", None, True),
    ("public_ipaddr", "ip", "{}", None, True),
    ("geolocation", "geoloc", "{}", None, True),
    ("num_reports", "reports", "{}", None, True),
    ("listed_gpu_cost", "gpuD_$/h", "{:0.2f}", None, True),
    ("min_bid_price", "gpuI$/h", "{:0.2f}", None, True),
    ("credit_discount_max", "rdisc", "{:0.2f}", None, True),
    ("listed_inet_up_cost",   "netu_$/TB", "{:0.2f}", lambda x: x * 1024, True),
    ("listed_inet_down_cost", "netd_$/TB", "{:0.2f}", lambda x: x * 1024, True),
    ("gpu_occupancy", "occup", "{}", None, True),
)

# These fields are displayed when you do 'show maints'
maintenance_fields = (
    ("machine_id", "Machine ID", "{}", None, True),
    ("start_time", "Start (Date/Time)", "{}", lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d/%H:%M'), True),
    ("end_time", "End (Date/Time)", "{}", lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d/%H:%M'), True),
    ("duration_hours", "Duration (Hrs)", "{}", None, True),
    ("maintenance_category", "Category", "{}", None, True),
)
ipaddr_fields = (
    ("ip", "ip", "{}", None, True),
    ("first_seen", "first_seen", "{}", None, True),
    ("first_location", "first_location", "{}", None, True),
)

audit_log_fields = (
    ("ip_address", "ip_address", "{}", None, True),
    ("api_key_id", "api_key_id", "{}", None, True),
    ("created_at", "created_at", "{}", None, True),
    ("api_route", "api_route", "{}", None, True),
    ("args", "args", "{}", None, True),
)
scheduled_jobs_fields = (
    ("id", "Scheduled Job ID", "{}", None, True),
    ("instance_id", "Instance ID", "{}", None, True),
    ("api_endpoint", "API Endpoint", "{}", None, True),
    ("start_time", "Start (Date/Time in UTC)", "{}", lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d/%H:%M'), True),
    ("end_time", "End (Date/Time in UTC)", "{}", lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d/%H:%M'), True),
    ("day_of_the_week", "Day of the Week", "{}", None, True),
    ("hour_of_the_day", "Hour of the Day in UTC", "{}", None, True),
    ("min_of_the_hour", "Minute of the Hour", "{}", None, True),
    ("frequency", "Frequency", "{}", None, True),
)
invoice_fields = (
    ("description", "Description", "{}", None, True),
    ("quantity", "Quantity", "{}", None, True),
    ("rate", "Rate", "{}", None, True),
    ("amount", "Amount", "{}", None, True),
    ("timestamp", "Timestamp", "{:0.1f}", None, True),
    ("type", "Type", "{}", None, True)
)
user_fields = (
    # ("api_key", "api_key", "{}", None, True),
    ("balance", "Balance", "{}", None, True),
    ("balance_threshold", "Bal. Thld", "{}", None, True),
    ("balance_threshold_enabled", "Bal. Thld Enabled", "{}", None, True),
    ("billaddress_city", "City", "{}", None, True),
    ("billaddress_country", "Country", "{}", None, True),
    ("billaddress_line1", "Addr Line 1", "{}", None, True),
    ("billaddress_line2", "Addr line 2", "{}", None, True),
    ("billaddress_zip", "Zip", "{}", None, True),
    ("billed_expected", "Billed Expected", "{}", None, True),
    ("billed_verified", "Billed Vfy", "{}", None, True),
    ("billing_creditonly", "Billing Creditonly", "{}", None, True),
    ("can_pay", "Can Pay", "{}", None, True),
    ("credit", "Credit", "{:0.2f}", None, True),
    ("email", "Email", "{}", None, True),
    ("email_verified", "Email Vfy", "{}", None, True),
    ("fullname", "Full Name", "{}", None, True),
    ("got_signup_credit", "Got Signup Credit", "{}", None, True),
    ("has_billing", "Has Billing", "{}", None, True),
    ("has_payout", "Has Payout", "{}", None, True),
    ("id", "Id", "{}", None, True),
    ("last4", "Last4", "{}", None, True),
    ("paid_expected", "Paid Expected", "{}", None, True),
    ("paid_verified", "Paid Vfy", "{}", None, True),
    ("password_resettable", "Pwd Resettable", "{}", None, True),
    ("paypal_email", "Paypal Email", "{}", None, True),
    ("ssh_key", "Ssh Key", "{}", None, True),
    ("user", "User", "{}", None, True),
    ("username", "Username", "{}", None, True)
)
connection_fields = (
    ("id", "ID", "{}", None, True),
    ("name", "NAME", "{}", None, True),
    ("cloud_type", "Cloud Type", "{}", None, True),
)


def version_string_sort(a, b) -> int:
    """
    Accepts two version strings and decides whether a > b, a == b, or a < b.
    This is meant as a sort function to be used for the driver versions in which only
    the == operator currently works correctly. Not quite finished...

    :param str a:
    :param str b:
    :return int:
    """
    a_parts = a.split(".")
    b_parts = b.split(".")

    return 0


offers_fields = {
    "bw_nvlink",
    "compute_cap",
    "cpu_arch",
    "cpu_cores",
    "cpu_cores_effective",
    "cpu_ghz",
    "cpu_ram",
    "cuda_max_good",
    "datacenter",
    "direct_port_count",
    "driver_version",
    "disk_bw",
    "disk_space",
    "dlperf",
    "dlperf_per_dphtotal",
    "dph_total",
    "duration",
    "external",
    "flops_per_dphtotal",
    "gpu_arch",
    "gpu_display_active",
    "gpu_frac",
    # "gpu_ram_free_min",
    "gpu_mem_bw",
    "gpu_name",
    "gpu_ram",
    "gpu_total_ram",
    "gpu_display_active",
    "gpu_max_power",
    "gpu_max_temp",
    "has_avx",
    "host_id",
    "id",
    "inet_down",
    "inet_down_cost",
    "inet_up",
    "inet_up_cost",
    "machine_id",
    "min_bid",
    "mobo_name",
    "num_gpus",
    "pci_gen",
    "pcie_bw",
    "reliability",
    #"reliability2",
    "rentable",
    "rented",
    "storage_cost",
    "static_ip",
    "total_flops",
    "ubuntu_version",
    "verification",
    "verified",
    "vms_enabled",
    "geolocation",
    "cluster_id"
}
offers_alias = {
    "cuda_vers": "cuda_max_good",
    "display_active": "gpu_display_active",
    #"reliability": "reliability2",
    "dlperf_usd": "dlperf_per_dphtotal",
    "dph": "dph_total",
    "flops_usd": "flops_per_dphtotal",
}
offers_mult = {
    "cpu_ram": 1000,
    "gpu_ram": 1000,
    "gpu_total_ram" : 1000,
    "duration": 24.0 * 60.0 * 60.0,
}
benchmarks_fields = {
    "contract_id",#             int        ID of instance/contract reporting benchmark
    "id",#                      int        benchmark unique ID
    "image",#                   string     image used for benchmark
    "last_update",#             float      date of benchmark
    "machine_id",#              int        id of machine benchmarked
    "model",#                   string     name of model used in benchmark
    "name",#                    string     name of benchmark
    "num_gpus",#                int        number of gpus used in benchmark
    "score"#                   float      benchmark score result
}
invoices_fields = {
    'id',#               int,                   
    'user_id',#          int,      
    'when',#             float,                     
    'paid_on',#          float,                     
    'payment_expected',# float,                     
    'amount_cents',#     int,                   
    'is_credit',#        bool,                   
    'is_delayed',#       bool,                   
    'balance_before',#   float,                     
    'balance_after',#    float,                     
    'original_amount',#  int,                   
    'event_id',#         string,                    
    'cut_amount',#       int,                   
    'cut_percent',#      float,                     
    'extra',#            json,           
    'service',#          string,                    
    'stripe_charge',#    json,           
    'stripe_refund',#    json,           
    'stripe_payout',#    json,           
    'error',#            json,           
    'paypal_email',#     string,                    
    'transfer_group',#   string,                    
    'failed',#           bool,                   
    'refunded',#         bool,                   
    'is_check',#         bool,                   
}
templates_fields = {
    "creator_id",#              int        ID of creator
    "created_at",#              float      time of initial template creation (UTC epoch timestamp)
    "count_created",#           int        #instances created (popularity)
    "default_tag",#             string     image default tag
    "docker_login_repo",#       string     image docker repository
    "id",#                      int        template unique ID
    "image",#                   string     image used for benchmark
    "jup_direct",#              bool       supports jupyter direct
    "hash_id",#                 string     unique hash ID of template
    "private",#                 bool       true: only your templates, None: public templates
    "name",#                    string     displayable name
    "recent_create_date",#      float      last time of instance creation (UTC epoch timestamp)
    "recommended_disk_space",#  float      min disk space required
    "recommended",#             bool       is templated on our recommended list
    "ssh_direct",#              bool       supports ssh direct
    "tag",#                     string     image tag
    "use_ssh",#                 string     supports ssh (direct or proxy)
}
TFA_METHOD_FIELDS = (
    ("id", "ID", "{}", None, True),
    ("user_id", "User ID", "{}", None, True),
    ("is_primary", "Primary", "{}", None, True),
    ("method", "Method", "{}", None, True),
    ("label", "Label", "{}", None, True),
    ("phone_number", "Phone Number", "{}", None, False),
    ("created_at", "Created", "{}", lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') if x else "N/A", True),
    ("last_used", "Last Used", "{}", lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') if x else "Never", True),
    ("fail_count", "Failures", "{}", None, True),
    ("locked_until", "Locked Until", "{}", lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') if x else "N/A", True),
)



##################################################################################################
# Query Parsing
##################################################################################################
def parse_query(query_str: str, res: Dict = None, fields = {}, field_alias = {}, field_multiplier = {}) -> Dict:
    """
    Basically takes a query string (like the ones in the examples of commands for the search__offers function) and
    processes it into a dict of URL parameters to be sent to the server.

    :param str query_str:
    :param Dict res:
    :return Dict:
    """
    if query_str is None:
        return res

    if res is None: res = {}
    if type(query_str) == list:
        query_str = " ".join(query_str)
    query_str = query_str.strip()

    # Revised regex pattern to accurately capture quoted strings, bracketed lists, and single words/numbers
    #pattern    = r"([a-zA-Z0-9_]+)\s*(=|!=|<=|>=|<|>| in | nin | eq | neq | not eq | not in )?\s*(\"[^\"]*\"|\[[^\]]+\]|[^ ]+)"
    #pattern    = "([a-zA-Z0-9_]+)( *[=><!]+| +(?:[lg]te?|nin|neq|eq|not ?eq|not ?in|in) )?( *)(\[[^\]]+\]|[^ ]+)?( *)"
    pattern     = r"([a-zA-Z0-9_]+)( *[=><!]+| +(?:[lg]te?|nin|neq|eq|not ?eq|not ?in|in) )?( *)(\[[^\]]+\]|\"[^\"]+\"|[^ ]+)?( *)"
    opts        = re.findall(pattern, query_str)

    #print("parse_query regex:")
    #print(opts)

    #print(opts)
    # res = {}
    op_names = {
        ">=": "gte",
        ">": "gt",
        "gt": "gt",
        "gte": "gte",
        "<=": "lte",
        "<": "lt",
        "lt": "lt",
        "lte": "lte",
        "!=": "neq",
        "==": "eq",
        "=": "eq",
        "eq": "eq",
        "neq": "neq",
        "noteq": "neq",
        "not eq": "neq",
        "notin": "notin",
        "not in": "notin",
        "nin": "notin",
        "in": "in",
    }


    joined = "".join("".join(x) for x in opts)
    if joined != query_str:
        raise ValueError(
            "Unconsumed text. Did you forget to quote your query? " + repr(joined) + " != " + repr(query_str))

    for field, op, _, value, _ in opts:
        value = value.strip(",[]")
        v = res.setdefault(field, {})
        op = op.strip()
        op_name = op_names.get(op)

        if field in field_alias:
            res.pop(field)
            field = field_alias[field]

        if (field == "driver_version") and ('.' in value):
            value = numeric_version(value)

        if not field in fields:
            print("Warning: Unrecognized field: {}, see list of recognized fields.".format(field), file=sys.stderr)
        if not op_name:
            raise ValueError("Unknown operator. Did you forget to quote your query? " + repr(op).strip("u"))
        if op_name in ["in", "notin"]:
            value = [x.strip() for x in value.split(",") if x.strip()]
        if not value:
            raise ValueError("Value cannot be blank. Did you forget to quote your query? " + repr((field, op, value)))
        if not field:
            raise ValueError("Field cannot be blank. Did you forget to quote your query? " + repr((field, op, value)))
        if value in ["?", "*", "any"]:
            if op_name != "eq":
                raise ValueError("Wildcard only makes sense with equals.")
            if field in v:
                del v[field]
            if field in res:
                del res[field]
            continue

        if isinstance(value, str):
            value = value.replace('_', ' ')
            value = value.strip('\"') 
        elif isinstance(value, list):
            value = [x.replace('_', ' ')    for x in value]
            value = [x.strip('\"')          for x in value]

        if field in field_multiplier:
            value = float(value) * field_multiplier[field]
            v[op_name] = value
        else:
            #print(value)
            if   (value == 'true') or (value == 'True'):
                v[op_name] = True
            elif (value == 'false') or (value == 'False'):
                v[op_name] = False
            elif (value == 'None') or (value == 'null'):
                v[op_name] = None
            else:
                v[op_name] = value

        if field not in res:
            res[field] = v
        else:
            res[field].update(v)
    #print(res)
    return res


##################################################################################################
# Display & Formatting
##################################################################################################
# ANSI escape codes for background/foreground colors
BG_DARK_GRAY = '\033[40m'  # Dark gray background
BG_LIGHT_GRAY = '\033[48;5;240m' # Light gray background
FG_WHITE = '\033[97m'            # Bright white text
BG_RESET = '\033[0m'             # Reset all formatting


def display_table(rows: list, fields: Tuple, replace_spaces: bool = True, auto_width: bool = True) -> None:
    """Basically takes a set of field names and rows containing the corresponding data and prints a nice tidy table
    of it.

    :param list rows: Each row is a dict with keys corresponding to the field names (first element) in the fields tuple.

    :param Tuple fields: 5-tuple describing a field. First element is field name, second is human readable version, third is format string, fourth is a lambda function run on the data in that field, fifth is a bool determining text justification. True = left justify, False = right justify. Here is an example showing the tuples in action.

    :rtype None:

    Example of 5-tuple: ("cpu_ram", "RAM", "{:0.1f}", lambda x: x / 1000, False)
    """
    header = [name for _, name, _, _, _ in fields]
    out_rows = [header]
    lengths = [len(x) for x in header]
    for instance in rows:
        row = []
        out_rows.append(row)
        for key, name, fmt, conv, _ in fields:
            conv = conv or (lambda x: x)
            val = instance.get(key, None)
            if val is None:
                s = "-"
            else:
                val = conv(val)
                s = fmt.format(val)
            if replace_spaces:
                s = s.replace(' ', '_')
            idx = len(row)
            lengths[idx] = max(len(s), lengths[idx])
            row.append(s)
    
    if auto_width:
        width = shutil.get_terminal_size((80, 20)).columns
        start_col_idxs = [0]
        total_len = 4  # +6ch for row label and -2ch for missing last sep in "  ".join()
        for i, l in enumerate(lengths):
            total_len += l + 2
            if total_len > width:
                start_col_idxs.append(i)  # index for the start of the next group
                total_len = l + 6         # l + 2 + the 4 from the initial length
        
        groups = {}
        for row in out_rows:
            grp_num = 0
            for i in range(len(start_col_idxs)):
                start = start_col_idxs[i]
                end = start_col_idxs[i+1] if i+1 < len(start_col_idxs) else len(lengths)
                groups.setdefault(grp_num, []).append(row[start:end])
                grp_num += 1
        
        for i, group in groups.items():
            idx = start_col_idxs[i]
            group_lengths = lengths[idx:idx+len(group[0])]
            for row_num, row in enumerate(group):
                bg_color = BG_DARK_GRAY if (row_num - 1) % 2 else BG_LIGHT_GRAY
                row_label = "  #" if row_num == 0 else f"{row_num:3d}"
                out = [row_label]
                for l, s, f in zip(group_lengths, row, fields[idx:idx+len(row)]):
                    _, _, _, _, ljust = f
                    if ljust: s = s.ljust(l)
                    else:     s = s.rjust(l)
                    out.append(s)
                print(bg_color + FG_WHITE + "  ".join(out) + BG_RESET)
            print()
    else:
        for row in out_rows:
            out = []
            for l, s, f in zip(lengths, row, fields):
                _, _, _, _, ljust = f
                if ljust:
                    s = s.ljust(l)
                else:
                    s = s.rjust(l)
                out.append(s)
            print("  ".join(out))


def print_or_page(args, text):
    """ Print text to terminal, or pipe to pager_cmd if too long. """
    line_threshold = shutil.get_terminal_size(fallback=(80, 24)).lines
    lines = text.splitlines()
    if not args.full and len(lines) > line_threshold:
        pager_cmd = ['less', '-R'] if shutil.which('less') else None
        if pager_cmd:
            proc = subprocess.Popen(pager_cmd, stdin=subprocess.PIPE)
            proc.communicate(input=text.encode())
            return True
        else:
            print(text)
            return False
    else:
        print(text)
        return False


def format_invoices_charges_results(args, results):
    indices_to_remove = []
    for i,item in enumerate(results):
        item['start'] = convert_timestamp_to_date(item['start']) if item['start'] else None
        item['end'] = convert_timestamp_to_date(item['end']) if item['end'] else None
        if item['amount'] == 0:
            indices_to_remove.append(i)  # Removing items that don't contribute to the total
        elif args.invoices:
            if item['type'] not in {'transfer', 'payout'}:
                item['amount'] *= -1  # present amounts intuitively as related to balance
            item['amount_str'] = f"${item['amount']:.2f}" if item['amount'] > 0 else f"-${abs(item['amount']):.2f}"
        else:
            item['amount'] = f"${item['amount']:.3f}"

        if args.charges:
            if item['type'] in {'instance','volume'} and not args.verbose:
                item['items'] = []  # Remove instance charge details if verbose is not set
            if item['source'] and '-' in item['source']:
                item['type'], item['source'] = item['source'].capitalize().split('-')
        
        item['items'] = format_invoices_charges_results(args, item['items'])
    
    for i in reversed(indices_to_remove):  # Remove in reverse order to avoid index shifting
        del results[i]
    
    return results


def rich_object_to_string(rich_obj, no_color=True):
    """ Render a Rich object (Table or Tree) to a string. """
    from rich.console import Console
    buffer = StringIO()  # Use an in-memory stream to suppress visible output
    console = Console(record=True, file=buffer)
    console.print(rich_obj)
    return console.export_text(clear=True, styles=not no_color)


def create_charges_tree(results, parent=None, title="Charges Breakdown"):
    """ Build and return a Rich Tree from nested charge results. """
    from rich.text import Text
    from rich.tree import Tree
    from rich.panel import Panel
    if parent is None:  # Create root node if this is the first call
        root = Tree(Text(title, style="bold red"))
        create_charges_tree(results, root)
        return Panel(root, style="white on #000000", expand=False)
    
    top_level = (parent.label.plain == title)
    for item in results:
        end_date = f" → {item['end']}" if item['start'] != item['end'] else ""
        label = Text.assemble(
            (item["type"], "bold cyan"),
            (f" {item['source']}" if item.get('source') else "", "gold1"), " → ",
            (f"{item['amount']}", 'bold green1' if top_level else 'green1'),
            (f" — {item['description']}", "bright_white" if top_level else "dim white"),
            (f"  ({item['start']}{end_date})", "bold bright_white" if top_level else "white")
        )
        node = parent.add(label, guide_style="blue3")
        if item.get("items"):
            create_charges_tree(item["items"], node)
    return parent


def create_rich_table_for_charges(args, results):
    """ Build and return a Rich Table from charge results. """
    from rich.table import Table
    from rich.text import Text
    from rich import box
    from rich.padding import Padding
    table = Table(style="white", header_style="bold bright_yellow", box=box.DOUBLE_EDGE, row_styles=["on grey11", "none"])
    table.add_column(Text("Type", justify="center"), style="bold steel_blue1", justify="center")
    table.add_column(Text("ID", justify="center"), style="gold1", justify="center")
    table.add_column(Text("Amount", justify="center"), style="sea_green2", justify="right")
    table.add_column(Text("Start", justify="center"), style="bright_white", justify="center")
    table.add_column(Text("End", justify="center"), style="bright_white", justify="center")
    if not args.charge_type or 'serverless' in args.charge_type:
        table.add_column(Text("Endpoint", justify="center"), style="bright_red", justify="center")
        table.add_column(Text("Workergroup", justify="center"), style="orchid", justify="center")
    for item in results:
        row = [item['type'].capitalize(), item['source'], item['amount'], item['start'], item['end']]
        if not args.charge_type or 'serverless' in args.charge_type:
            row.append(str(item['metadata'].get('endpoint_id', '')))
            row.append(str(item['metadata'].get('workergroup_id', '')))
        table.add_row(*row)
    return Padding(table, (1, 2), style="on #000000", expand=False)  # Print with a black background


def create_rich_table_for_invoices(results):
    """ Build and return a Rich Table from invoice results. """
    from rich.table import Table
    from rich.text import Text
    from rich import box
    from rich.padding import Padding
    invoice_type_to_color = {
        "credit": "green1",
        "transfer": "gold1",
        "payout": "orchid",
        "reserved": "sky_blue1",
        "refund": "bright_red",
    }
    table = Table(style="white", header_style="bold bright_yellow", box=box.DOUBLE_EDGE, row_styles=["on grey11", "none"])
    table.add_column(Text("ID", justify="center"), style="bright_white", justify="center")
    table.add_column(Text("Created", justify="center"), style="yellow3", justify="center")
    table.add_column(Text("Paid", justify="center"), style="yellow3", justify="center")
    table.add_column(Text("Type", justify="center"), justify="center")
    table.add_column(Text("Result", justify="center"), justify="right")
    table.add_column(Text("Source", justify="center"), style="bright_cyan", justify="center")
    table.add_column(Text("Description", justify="center"), style="bright_white", justify="left")
    for item in results:
        table.add_row(
            str(item['metadata']['invoice_id']),
            item['start'],
            item['end'] if item['end'] else 'N/A',
            Text(item['type'].capitalize(), style=invoice_type_to_color.get(item['type'], "white")),
            Text(item['amount_str'], style="sea_green2" if item['amount'] > 0 else "bright_red"),
            item['source'].capitalize() if item['type'] != 'transfer' else item['source'],
            item['description'],
        )
    return Padding(table, (1, 2), style="on #000000", expand=False)  # Print with a black background


def create_rich_table_from_rows(rows, headers=None, title='', sort_key=None):
    """ (Generic) Creates a Rich table from a list of dict rows. """
    from rich import box
    from rich.table import Table
    if not isinstance(rows, list):
        raise ValueError("Invalid Data Type: rows must be a list")
    # Handle list of dictionaries
    if isinstance(rows[0], dict):
        headers = headers or list(rows[0].keys())
        rows = [[row_dict.get(h, "") for h in headers] for row_dict in rows]
    elif headers is None:
        raise ValueError("Headers must be provided if rows are not dictionaries")
    # Sort rows if requested
    if sort_key:
        rows = sorted(rows, key=sort_key)
    # Create the Rich table
    table = Table(title=title, style="white", header_style="bold bright_yellow", box=box.DOUBLE_EDGE)
    # Add columns
    for header in headers:
        # You can customize alignment and style here per column
        table.add_column(header, justify="left", style="bright_white", no_wrap=True)
    # Add rows
    for row in rows:
        # Convert everything to string to avoid type issues
        table.add_row(*[str(cell) for cell in row])
    return table


DEFAULT_INSTANCE_SELECT_COLS = [
    "id", "actual_status", "label",
    "num_gpus", "gpu_name", "gpu_util",
    "disk_space", "disk_usage", "disk_util",
    "volume_info",
    "dph_total", "image_uuid",
    "start_date", "verification",
]

VERBOSE_INSTANCE_SELECT_COLS = DEFAULT_INSTANCE_SELECT_COLS + [
    "machine_id", "template_id", "template_name",
    "geolocation", "inet_up", "inet_down",
    "ssh_host", "ssh_port", "status_msg",
]


def _fmt_age(start_date):
    """Format seconds elapsed since start_date as e.g. '2d 3h' or '4h 15m'."""
    if not start_date:
        return "—"
    secs = max(0, time.time() - start_date)
    d, rem  = divmod(int(secs), 86400)
    h, rem  = divmod(rem, 3600)
    m, _    = divmod(rem, 60)
    if d:   return f"{d}d {h}h"
    if h:   return f"{h}h {m}m"
    return f"{m}m"


def _fmt_disk(disk_usage, disk_space, disk_util):
    """Format disk as 'used/total GB (X%)' or '?/total GB'."""
    total = f"{disk_space:.0f}" if disk_space is not None else "?"
    if disk_usage is None or disk_usage < 0:
        return f"?/{total} GB"
    used = f"{disk_usage:.1f}"
    if disk_util is not None and disk_util >= 0:
        pct = disk_util * 100
        return f"{used}/{total} GB ({pct:.0f}%)"
    return f"{used}/{total} GB"


def _fmt_volumes(volume_info):
    """Format volume_info list as a compact string showing IDs and usage."""
    if not volume_info:
        return "—"
    if len(volume_info) == 1:
        v = volume_info[0]
        vid = v.get("id", "?")
        avail = v.get("avail_space")
        total = v.get("total_space")
        if avail is not None and total is not None:
            used = total - avail
            return f"#{vid} {used:.0f}/{total:.0f} GB"
        return f"#{vid}"
    # Multiple volumes: list all IDs
    return ", ".join(f"#{v.get('id', '?')}" for v in volume_info)


def _fmt_gpu(num_gpus, gpu_name, gpu_util):
    """Format as '4x RTX 3090' or '4x RTX 3090 (72%)'."""
    base = f"{int(num_gpus)}x {gpu_name}" if num_gpus and gpu_name else (gpu_name or "—")
    if gpu_util is not None and gpu_util >= 0:
        return f"{base} ({gpu_util:.0f}%)"
    return base

STATUS_COLORS = {"running": "bold green", "loading": "bold yellow", "exited": "bright_red", "created": "bright_white"}
_VERIF_COLORS  = {"verified": "sea_green2", "unverified": "gold1", "deverified": "bright_red"}


def _status_style(status):
    return STATUS_COLORS.get(status, "white")


def _verif_style(v):
    return _VERIF_COLORS.get(v, "white")


# max_width caps Rich column expansion; also used by _estimate_table_width so estimate >= actual
_INSTANCE_COL_MAX_WIDTHS = {
    "gpu":      20,   # "8x NVIDIA GTX 1080 Ti" = 21 chars; cap at 20 to keep estimate accurate
    "image":    30,   # matches min_width so estimate == actual rendering width
    "age":       8,   # "XXXd XXh" = 8 chars
    "volumes":  17,   # "used/total GB (label)" capped so 10-col table fits at 150 cols
    "location": 22,   # "California, USA!-55-84-51-63" = 28 chars; cap to keep table in bounds
    "net":      11,   # "↑1000 ↓1000" = 11 chars
    "ssh":      22,   # "ssh2281.vast.ai:13912" style
    "template": 32,
    "msg":      30,
}

# min_width: minimum content width (chars) used for fit estimation and as Rich min_width
# drop_order 0 = never drop; higher = drop sooner when terminal is narrow
# Priority (drop first → last): ssh > volumes > disk > verified > age > image > $/hr > never

# Column spec: (name, header, style, justify, min_width, drop_order, verbose_only)
INSTANCE_COL_SPECS = [
    ("id",       "ID",       "bright_white", "right",  4,   0,  False),
    ("status",   "Status",   None,           "center", 7,   0,  False),
    ("label",    "Label",    "bright_white", "left",   7,   0,  False),
    ("gpu",      "GPU",      "steel_blue1",  "left",   13,  0,  False),
    ("disk",     "Disk",     "bright_white", "right",  8,   5,  False),
    ("volumes",  "Volumes",  "bright_white", "left",   10,  6,  False),  # gated by show_volumes
    ("dph",      "$/hr",     "sea_green2",   "right",  7,   1,  False),
    ("image",    "Image",    "orchid",       "left",   30,  2,  False),
    ("age",      "Age",      "bright_white", "left",   8,   3,  False),
    ("verified", "Verified", None,           "center", 10,  0,  False),
    # verbose-only columns (drop order continues from 8+)
    ("machine",  "Machine",  "gold1",        "center", 5,   8,  True),
    ("net",      "Net Mbps", "bright_white", "left",   9,   9,  True),
    ("location", "Location", "bright_white", "center", 10,  10, True),
    ("template", "Template", "bright_white", "center", 20,  11, True),
    ("ssh",      "SSH",      "cyan",         "left",   21,  7,  True),
    ("msg",      "Msg",      "dim white",    "left",   15,  12, True),
]

_INSTANCE_COL_SPEC_BY_NAME = {s[0]: s for s in INSTANCE_COL_SPECS}
try:
    from rich.text import Text as _RichText
except ImportError:
    _RichText = None  # type: ignore


def _render_instance_col(name, inst):
    """Render a single cell value for the given column name."""
    if name == "id":
        return str(inst.get("id", "—"))
    if name == "status":
        s = inst.get("actual_status") or "—"
        return _RichText(s, style=_status_style(s))
    if name == "label":
        return inst.get("label") or "—"
    if name == "gpu":
        return _fmt_gpu(inst.get("num_gpus"), inst.get("gpu_name"), inst.get("gpu_util"))
    if name == "disk":
        return _fmt_disk(inst.get("disk_usage"), inst.get("disk_space"), inst.get("disk_util"))
    if name == "volumes":
        return _fmt_volumes(inst.get("volume_info") or [])
    if name == "dph":
        dph = inst.get("dph_total")
        return f"${dph:.4f}" if dph is not None else "—"
    if name == "image":
        return (inst.get("image_uuid") or "—")[:50]
    if name == "age":
        return _fmt_age(inst.get("start_date"))
    if name == "verified":
        v = inst.get("verification") or "—"
        return _RichText(v, style=_verif_style(v))
    if name == "ssh":
        return f"{inst.get('ssh_host')}:{inst.get('ssh_port', '')}" if inst.get("ssh_host") else "—"
    if name == "machine":
        return str(inst.get("machine_id", "—"))
    if name == "net":
        up, down = inst.get("inet_up"), inst.get("inet_down")
        return f"↑{up:.0f} ↓{down:.0f}" if (up is not None and down is not None) else "—"
    if name == "location":
        return inst.get("geolocation") or "—"
    if name == "template":
        tid, tname = inst.get("template_id"), inst.get("template_name") or ""
        return (f"{tname[:28]} ({tid})" if tid else tname[:30] or "—")
    if name == "msg":
        return (inst.get("status_msg") or "—")[:40]
    return "—"


def _estimate_table_width(specs):
    """Estimate rendered table width for a list of col specs.
    Uses _INSTANCE_COL_MAX_WIDTHS when available so estimate >= actual rendered width.
    Formula: Padding(2) + outer borders(2) + per-col cell padding(2) + separators(n-1) + content
    """
    n = len(specs)
    content = sum(
        _INSTANCE_COL_MAX_WIDTHS.get(s[0]) or max(len(s[1]), s[4])
        for s in specs
    )
    return 4 + 2 * n + (n - 1) + content


def build_instances_table(instances, verbose=False, cols=None):
    """Build the Rich table for instances.

    cols: optional list of column name strings (overrides auto-selection).
    Returns (Padding, hidden_headers) where hidden_headers lists auto-dropped column headers.
    """
    import shutil
    from rich.table import Table
    from rich import box

    show_volumes = any(inst.get("volume_info") for inst in instances)
    term_width = shutil.get_terminal_size((120, 24)).columns

    if cols is not None:
        # User-specified columns: look up specs by name, preserve requested order
        active = [_INSTANCE_COL_SPEC_BY_NAME[c] for c in cols if c in _INSTANCE_COL_SPEC_BY_NAME]
        hidden = []
    else:
        # Auto-selection: start with all applicable columns
        candidate = [
            s for s in INSTANCE_COL_SPECS
            if (not s[6] or verbose) and not (s[0] == "volumes" and not show_volumes)
        ]
        # Drop lowest-priority columns (highest drop_order, skip drop_order==0) until it fits
        droppable = sorted((s for s in candidate if s[5] > 0), key=lambda s: s[5], reverse=True)
        active = list(candidate)
        for drop_spec in droppable:
            if _estimate_table_width(active) <= term_width:
                break
            active.remove(drop_spec)
        hidden = [s[1] for s in candidate if s not in active]  # headers of dropped cols

    tbl = Table(
        style="white",
        header_style="bold bright_yellow",
        box=box.DOUBLE_EDGE,
        row_styles=["on grey11", "none"],
    )
    for name, header, style, justify, min_width, *_ in active:
        kwargs = dict(justify=justify, no_wrap=True, min_width=min_width)
        if name in _INSTANCE_COL_MAX_WIDTHS:
            kwargs["max_width"] = _INSTANCE_COL_MAX_WIDTHS[name]
        if style:
            kwargs["style"] = style
        tbl.add_column(_RichText(header, justify="center"), **kwargs)

    for inst in instances:
        tbl.add_row(*[_render_instance_col(name, inst) for name, *_ in active])

    return tbl, hidden


def build_summary_panel(total, label_counts, active_filters=None, order_by=None):
    """Build a Rich Panel summarising the instance query.

    active_filters: dict of {key: [values]} for display
    order_by: list of {"col": str, "dir": "asc"|"desc"} dicts
    """
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    lines = []

    # Total
    lines.append(Text.assemble(("Total: ", "bold bright_yellow"), (f"{total} instances", "bold bright_white")))

    # Label breakdown from label_counts
    if label_counts:
        parts = []
        for lbl, cnt in sorted(label_counts.items(), key=lambda x: -x[1]):
            display = lbl if lbl else "(unlabeled)"
            parts.append(f"{display}: {cnt}")
        lines.append(Text.assemble(("Labels: ", "bold bright_yellow"), ("  ·  ".join(parts), "bright_white")))

    # Active filter line
    if active_filters:
        filter_line = Text.assemble(("Filters: ", "bold bright_yellow"))
        for i, (k, vals) in enumerate(active_filters.items()):
            if i: filter_line.append("   ", style="dim")
            filter_line.append(f"{k}=", style="bold bright_white")
            filter_line.append_text(_render_filter_values(vals, _FILTER_VALUE_COLORS.get(k), bold=True, line_sep=True))
        lines.append(filter_line)

    # Active order-by line
    if order_by:
        order_line = Text.assemble(("Order by: ", "bold bright_yellow"))
        for i, key in enumerate(order_by):
            if i: order_line.append("  >  ", style="bright_white")
            order_line.append(key["col"], style="bold bright_white")
            order_line.append(f" ({key['dir']})", style="bright_white")
        lines.append(order_line)

    grid = Table.grid(padding=(0, 0))
    grid.add_column()
    for line in lines:
        grid.add_row(line)

    return Panel(grid, title="[bold bright_yellow]Results Summary[/bold bright_yellow]", style="on #000000", border_style="bright_yellow", expand=False)


def _render_filter_values(values, colors=None, bold=False, line_sep=False):
    """Render a sequence of filter values as a Rich Text, dot-separated, with optional per-value colors."""
    t = _RichText()
    for i, v in enumerate(values):
        if i: t.append("|" if line_sep else "  ·  ", style="bright_white")
        style = (colors or {}).get(v, "bright_white")
        t.append(v, style=("bold " + style) if bold else style)
    return t


# Maps active_display_filters keys to their per-value color dicts (absent = bright_white)
_FILTER_VALUE_COLORS = {
    "status":       STATUS_COLORS,
    "verification": _VERIF_COLORS,
}


def build_filters_panel(filters):
    """Build a Rich Panel showing the distinct filterable values from /instances/filters/."""
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    statuses = sorted({f["actual_status"] for f in filters if f.get("actual_status")})
    verifs   = sorted({f["verification"]   for f in filters if f.get("verification")})
    gpus     = sorted({f["gpu_name"]        for f in filters if f.get("gpu_name")})

    lines = [
        Text.assemble(("--status:       ", "bold bright_yellow"), _render_filter_values(statuses, STATUS_COLORS)),
        Text.assemble(("--verification: ", "bold bright_yellow"), _render_filter_values(verifs, _VERIF_COLORS)),
        Text.assemble(("--gpu-name:     ", "bold bright_yellow"), _render_filter_values(gpus)),
    ]

    grid = Table.grid(padding=(0, 0))
    grid.add_column()
    for line in lines:
        grid.add_row(line)

    return Panel(grid, title="[bright_white]Filterable Values[/bright_white]", style="on #000000", border_style="bright_white", expand=False)


##################################################################################################
# URL Parsing & SSH
##################################################################################################
class VRLException(Exception):
    pass


def parse_vast_url(url_str):
    """
    Breaks up a vast-style url in the form instance_id:path and does
    some basic sanity type-checking.

    :param url_str:
    :return:
    """

    instance_id = None
    path = url_str
    #print(f'url_str: {url_str}')
    if (":" in url_str):
        url_parts = url_str.split(":", 2)
        if len(url_parts) == 2:
            (instance_id, path) = url_parts
        else:
            raise VRLException("Invalid VRL (Vast resource locator).")
    else:
        try:
            instance_id = int(path)
            path = "/"
        except:
            pass

    valid_unix_path_regex = re.compile('^(/)?([^/\0]+(/)?)+$')
    # Got this regex from https://stackoverflow.com/questions/537772/what-is-the-most-correct-regular-expression-for-a-unix-file-path
    if (path != "/") and (valid_unix_path_regex.match(path) is None):
        raise VRLException(f"Path component: {path} of VRL is not a valid Unix style path.")
    
    #print(f'instance_id: {instance_id}')
    #print(f'path: {path}')
    return (instance_id, path)


def get_ssh_key(argstr):
    ssh_key = argstr
    # Including a path to a public key is pretty reasonable.
    if os.path.exists(argstr):
      with open(argstr) as f:
        ssh_key = f.read()

    if "PRIVATE KEY" in ssh_key:
      raise ValueError(deindent("""
        🐴 Woah, hold on there, partner!

        That's a *private* SSH key.  You need to give the *public* 
        one. It usually starts with 'ssh-rsa', is on a single line, 
        has around 200 or so "base64" characters and ends with 
        some-user@some-where. "Generate public ssh key" would be 
        a good search term if you don't know how to do this.
      """, add_separator=False))

    if not ssh_key.lower().startswith('ssh'):
      raise ValueError(deindent("""
        Are you sure that's an SSH public key?

        Usually it starts with the stanza 'ssh-(keytype)' 
        where the keytype can be things such as rsa, ed25519-sk, 
        or dsa. What you passed me was:

        {}

        And welp, that just don't look right.
      """.format(ssh_key), add_separator=False))

    return ssh_key


def ssh_url(args, protocol):

    json_object = None

    # Opening JSON file
    try:
        with open(f"{DIRS['temp']}/ssh_{args.id}.json", 'r') as openfile:
            json_object = json.load(openfile)
    except:
        pass

    port      = None
    ipaddr    = None

    if json_object is not None:
        ipaddr = json_object["ipaddr"]
        port   = json_object["port"]

    if ipaddr is None or ipaddr.endswith('.vast.ai'):
        req_url = apiurl(args, "/instances", {"owner": "me"})
        r = http_get(args, req_url)
        r.raise_for_status()
        rows = r.json()["instances"]

        if args.id:
            matches = [r for r in rows if r['id'] == args.id]
            if not matches:
                print(f"error: no instance found with id {args.id}")
                return 1
            instance = matches[0]
        elif len(rows) > 1:
            print("Found multiple running instances")
            return 1
        else:
            instance = rows[0]

        ports     = instance.get("ports",{})
        port_22d  = ports.get("22/tcp",None)
        port      = -1
        try:
            if (port_22d is not None):
                ipaddr = instance["public_ipaddr"]
                port   = int(port_22d[0]["HostPort"])
            else:        
                ipaddr = instance["ssh_host"]
                port   = int(instance["ssh_port"])+1 if "jupyter" in instance["image_runtype"] else int(instance["ssh_port"])
        except:
            port = -1

    if (port > 0):
        print(f'{protocol}root@{ipaddr}:{port}')
    else:
        print(f'error: ssh port not found')

   
    # Writing to sample.json
    try:
        with open(f"{DIRS['temp']}/ssh_{args.id}.json", "w") as outfile:
            json.dump({"ipaddr":ipaddr, "port":port}, outfile)
    except:
        pass


##################################################################################################
# Date & Time Helpers
##################################################################################################
def default_start_date():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def default_end_date():
    return (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%d")


def convert_timestamp_to_date(unix_timestamp):
    utc_datetime = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
    return utc_datetime.strftime("%Y-%m-%d")


def parse_day_cron_style(value):
    """
    Accepts an integer string 0-6 or '*' to indicate 'Every day'.
    Returns 0-6 as int, or None if '*'.
    """
    val = str(value).strip()
    if val == "*":
        return None
    try:
        day = int(val)
        if 0 <= day <= 6:
            return day
    except ValueError:
        pass
    raise argparse.ArgumentTypeError("Day must be 0-6 (0=Sunday) or '*' for every day.")


def parse_hour_cron_style(value):
    """
    Accepts an integer string 0-23 or '*' to indicate 'Every hour'.
    Returns 0-23 as int, or None if '*'.
    """
    val = str(value).strip()
    if val == "*":
        return None
    try:
        hour = int(val)
        if 0 <= hour <= 23:
            return hour
    except ValueError:
        pass
    raise argparse.ArgumentTypeError("Hour must be 0-23 or '*' for every hour.")


def convert_dates_to_timestamps(args):
    selector_flag = ""
    end_timestamp = time.time()
    start_timestamp = time.time() - (24*60*60)
    start_date_txt = ""
    end_date_txt = ""

    import dateutil
    from dateutil import parser

    if args.end_date:
        try:
            end_date = dateutil.parser.parse(str(args.end_date))
            end_date_txt = end_date.isoformat()
            end_timestamp = time.mktime(end_date.timetuple())
        except ValueError as e:
            print(f"Warning: Invalid end date format! Ignoring end date! \n {str(e)}")
    
    if args.start_date:
        try:
            start_date = dateutil.parser.parse(str(args.start_date))
            start_date_txt = start_date.isoformat()
            start_timestamp = time.mktime(start_date.timetuple())
        except ValueError as e:
            print(f"Warning: Invalid start date format! Ignoring end date! \n {str(e)}")

    return start_timestamp, end_timestamp


##################################################################################################
# Scheduling Helpers
##################################################################################################
def validate_frequency_values(day_of_the_week, hour_of_the_day, frequency):

    # Helper to raise an error with a consistent message.
    def raise_frequency_error():
        msg = ""
        if frequency == "HOURLY":
            msg += "For HOURLY jobs, day and hour must both be \"*\"."
        elif frequency == "DAILY":
            msg += "For DAILY jobs, day must be \"*\" and hour must have a value between 0-23."
        elif frequency == "WEEKLY":
            msg += "For WEEKLY jobs, day must have a value between 0-6 and hour must have a value between 0-23."
        sys.exit(msg)

    if frequency == "HOURLY":
        if not (day_of_the_week is None and hour_of_the_day is None):
            raise_frequency_error()
    if frequency == "DAILY":
        if not (day_of_the_week is None and hour_of_the_day is not None):
            raise_frequency_error()
    if frequency == "WEEKLY":
        if not (day_of_the_week is not None and hour_of_the_day is not None):
            raise_frequency_error()


def add_scheduled_job(args, req_json, cli_command, api_endpoint, request_method, instance_id, contract_end_date):
    start_timestamp, end_timestamp = convert_dates_to_timestamps(args)
    if args.end_date is None:
        end_timestamp=contract_end_date
        args.end_date = convert_timestamp_to_date(contract_end_date)

    if start_timestamp >= end_timestamp:
        raise ValueError("--start_date must be less than --end_date.")

    day, hour, frequency = args.day, args.hour, args.schedule

    schedule_job_url = apiurl(args, f"/commands/schedule_job/")

    request_body = {
                "start_time": start_timestamp, 
                "end_time": end_timestamp, 
                "api_endpoint": api_endpoint,
                "request_method": request_method,
                "request_body": req_json,
                "day_of_the_week": day,
                "hour_of_the_day": hour,
                "frequency": frequency,
                "instance_id": instance_id
            }
                # Send a POST request
    response = requests.post(schedule_job_url, headers=headers, json=request_body)

    if args.explain:
        print("request json: ")
        print(request_body)

        # Handle the response based on the status code
    if response.status_code == 200:
        print(f"add_scheduled_job insert: success - Scheduling {frequency} job to {cli_command} from {args.start_date} UTC to {args.end_date} UTC")
    elif response.status_code == 401:
        print(f"add_scheduled_job insert: failed status_code: {response.status_code}. It could be because you aren't using a valid api_key.")
    elif response.status_code == 422:
        user_input = input("Existing scheduled job found. Do you want to update it (y|n)? ")
        if user_input.strip().lower() == "y":
            scheduled_job_id = response.json()["scheduled_job_id"]
            schedule_job_url = apiurl(args, f"/commands/schedule_job/{scheduled_job_id}/")
            response = update_scheduled_job(cli_command, schedule_job_url, frequency, args.start_date, args.end_date, request_body)
        else:
            print("Job update aborted by the user.")
    else:
            # print(r.text)
        print(f"add_scheduled_job insert: failed error: {response.status_code}. Response body: {response.text}")        


def update_scheduled_job(cli_command, schedule_job_url, frequency, start_date, end_date, request_body):
    response = requests.put(schedule_job_url, headers=headers, json=request_body)

        # Raise an exception for HTTP errors
    response.raise_for_status()
    if response.status_code == 200:
        print(f"add_scheduled_job update: success - Scheduling {frequency} job to {cli_command} from {start_date} UTC to {end_date} UTC")
        print(response.json())
    elif response.status_code == 401:
        print(f"add_scheduled_job update: failed status_code: {response.status_code}. It could be because you aren't using a valid api_key.")
    else:
            # print(r.text)
        print(f"add_scheduled_job update: failed status_code: {response.status_code}.")
        print(response.json())

    return response


def normalize_schedule_fields(job):
    """
    Mutates the job dict to replace None values with readable scheduling labels.
    """
    if job.get("day_of_the_week") is None:
        job["day_of_the_week"] = "Everyday"
    else:
        days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        job["day_of_the_week"] = days[int(job["day_of_the_week"])]
    
    if job.get("hour_of_the_day") is None:
        job["hour_of_the_day"] = "Every hour"
    else:
        hour = int(job["hour_of_the_day"])
        suffix = "AM" if hour < 12 else "PM"
        hour_12 = hour % 12
        hour_12 = 12 if hour_12 == 0 else hour_12
        job["hour_of_the_day"] = f"{hour_12}_{suffix}"

    if job.get("min_of_the_hour") is None:
        job["min_of_the_hour"] = "Every minute"
    else:
        job["min_of_the_hour"] = f"{int(job['min_of_the_hour']):02d}"
    
    return job


def normalize_jobs(jobs):
    """
    Applies normalization to a list of job dicts.
    """
    return [normalize_schedule_fields(job) for job in jobs]


##################################################################################################
# Instance Lifecycle Helpers
##################################################################################################
def get_runtype(args):
    runtype = 'ssh'
    if args.args:
        runtype = 'args'
    if (args.args == '') or (args.args == ['']) or (args.args == []):
        runtype = 'args'
        args.args = None
    if not args.jupyter and (args.jupyter_dir or args.jupyter_lab):
        args.jupyter = True
    if args.jupyter and runtype == 'args':
        print("Error: Can't use --jupyter and --args together. Try --onstart or --onstart-cmd instead of --args.", file=sys.stderr)
        return 1

    if args.jupyter:
        runtype = 'jupyter_direc ssh_direc ssh_proxy' if args.direct else 'jupyter_proxy ssh_proxy'
    elif args.ssh:
        runtype = 'ssh_direc ssh_proxy' if args.direct else 'ssh_proxy'

    return runtype


def validate_volume_params(args):
    if args.volume_size and not args.create_volume:
        raise argparse.ArgumentTypeError("Error: --volume-size can only be used with --create-volume. Please specify a volume ask ID to create a new volume of that size.")
    if (args.create_volume or args.link_volume) and not args.mount_path:
        raise argparse.ArgumentTypeError("Error: --mount-path is required when creating or linking a volume.")

    # This regex matches absolute or relative Linux file paths (no null bytes)
    valid_linux_path_regex = re.compile(r'^(/)?([^/\0]+(/)?)+$')
    if not valid_linux_path_regex.match(args.mount_path):
        raise argparse.ArgumentTypeError(f"Error: --mount-path '{args.mount_path}' is not a valid Linux file path.")
    
    volume_info = {
        "mount_path": args.mount_path,
        "create_new": True if args.create_volume else False,
        "volume_id": args.create_volume if args.create_volume else args.link_volume
    }
    if args.volume_label:
        volume_info["name"] = args.volume_label
    if args.volume_size:
        volume_info["size"] = args.volume_size
    elif args.create_volume:  # If creating a new volume and size is not passed in, default size is 15GB
        volume_info["size"] = 15

    return volume_info


def validate_portal_config(json_blob):
    # jupyter runtypes already self-correct
    if 'jupyter' in json_blob['runtype']:
        return
    
    # remove jupyter configs from portal_config if not a jupyter runtype
    portal_config = json_blob['env']['PORTAL_CONFIG'].split("|")
    filtered_config = [config_str for config_str in portal_config if 'jupyter' not in config_str.lower()]
    
    if not filtered_config:
        raise ValueError("Error: env variable PORTAL_CONFIG must contain at least one non-jupyter related config string if runtype is not jupyter")
    else:
        json_blob['env']['PORTAL_CONFIG'] = "|".join(filtered_config)


def generate_ssh_key(auto_yes=False):
    """
    Generate a new SSH key pair using ssh-keygen and return the public key content.
    
    Args:
        auto_yes (bool): If True, automatically answer yes to prompts
    
    Returns:
        str: The content of the generated public key
        
    Raises:
        SystemExit: If ssh-keygen is not available or key generation fails
    """
    
    print("No SSH key provided. Generating a new SSH key pair and adding public key to account...")
    
    # Define paths
    ssh_dir = Path.home() / '.ssh'
    private_key_path = ssh_dir / 'id_ed25519'
    public_key_path = ssh_dir / 'id_ed25519.pub'
    
    # Create .ssh directory if it doesn't exist
    try:
        ssh_dir.mkdir(mode=0o700, exist_ok=True)
    except OSError as e:
        print(f"Error creating .ssh directory: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Check if any part of the key pair already exists and backup if needed
    if private_key_path.exists() or public_key_path.exists():
        print(f"An SSH key pair 'id_ed25519' already exists in {ssh_dir}")
        if auto_yes:
            print("Auto-answering yes to backup existing key pair.")
            response = 'y'
        else:
            response = input("Would you like to generate a new key pair and backup your existing id_ed25519 key pair. [y/N]: ").lower()
        if response not in ['y', 'yes']:
            print("Aborted. No new key generated.")
            sys.exit(0)
        
        # Generate timestamp for backup
        timestamp = int(time.time())
        backup_private_path = ssh_dir / f'id_ed25519.backup_{timestamp}'
        backup_public_path = ssh_dir / f'id_ed25519.pub.backup_{timestamp}'
        
        try:
            # Backup existing private key if it exists
            if private_key_path.exists():
                private_key_path.rename(backup_private_path)
                print(f"Backed up existing private key to: {backup_private_path}")
            
            # Backup existing public key if it exists
            if public_key_path.exists():
                public_key_path.rename(backup_public_path)
                print(f"Backed up existing public key to: {backup_public_path}")
                
        except OSError as e:
            print(f"Error backing up existing SSH keys: {e}", file=sys.stderr)
            sys.exit(1)
        
        print("Generating new SSH key pair and adding public key to account...")
    
    # Check if ssh-keygen is available
    try:
        subprocess.run(['ssh-keygen', '--help'], capture_output=True, check=False)
    except FileNotFoundError:
        print("Error: ssh-keygen not found. Please install OpenSSH client tools.", file=sys.stderr)
        sys.exit(1)
    
    # Generate the SSH key pair
    try:
        cmd = [
            'ssh-keygen',
            '-t', 'ed25519',       # Ed25519 key type
            '-f', str(private_key_path),  # Output file path
            '-N', '',              # Empty passphrase
            '-C', f'{os.getenv("USER") or os.getenv("USERNAME", "user")}-vast.ai'  # User
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            input='y\n',           # Automatically answer 'yes' to overwrite prompts
            check=True
        )
        
    except subprocess.CalledProcessError as e:
        print(f"Error generating SSH key: {e}", file=sys.stderr)
        if e.stderr:
            print(f"ssh-keygen error: {e.stderr}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error during key generation: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Set proper permissions for the private key
    try:
        private_key_path.chmod(0o600)  # Read/write for owner only
    except OSError as e:
        print(f"Warning: Could not set permissions for private key: {e}", file=sys.stderr)
    
    # Read and return the public key content
    try:
        with open(public_key_path, 'r') as f:
            public_key_content = f.read().strip()
        
        return public_key_content
        
    except IOError as e:
        print(f"Error reading generated public key: {e}", file=sys.stderr)
        sys.exit(1)


def get_template_arguments():
    return [
        argument("--name", help="name of the template", type=str),
        argument("--image", help="docker container image to launch", type=str),
        argument("--image_tag", help="docker image tag (can also be appended to end of image_path)", type=str),
        argument("--href", help="link you want to provide", type=str),
        argument("--repo", help="link to repository", type=str),
        argument("--login", help="docker login arguments for private repo authentication, surround with ''", type=str),
        argument("--env", help="Contents of the 'Docker options' field", type=str),
        argument("--ssh", help="Launch as an ssh instance type", action="store_true"),
        argument("--jupyter", help="Launch as a jupyter instance instead of an ssh instance", action="store_true"),
        argument("--direct", help="Use (faster) direct connections for jupyter & ssh", action="store_true"),
        argument("--jupyter-dir", help="For runtype 'jupyter', directory in instance to use to launch jupyter. Defaults to image's working directory", type=str),
        argument("--jupyter-lab", help="For runtype 'jupyter', Launch instance with jupyter lab", action="store_true"),
        argument("--onstart-cmd", help="contents of onstart script as single argument", type=str),
        argument("--search_params", help="search offers filters", type=str),
        argument("-n", "--no-default", action="store_true", help="Disable default search param query args"),
        argument("--disk_space", help="disk storage space, in GB", type=str),
        argument("--readme", help="readme string", type=str),
        argument("--hide-readme", help="hide the readme from users", action="store_true"),
        argument("--desc", help="description string", type=str),
        argument("--public", help="make template available to public", action="store_true"),
    ]


def destroy_instance(id,args):
    url = apiurl(args, "/instances/{id}/".format(id=id))
    r = http_del(args, url, headers=headers,json={})
    r.raise_for_status()
    if args.raw:
        return r
    elif (r.status_code == 200):
        rj = r.json()
        if (rj["success"]):
            print("destroying instance {id}.".format(**(locals())))
        else:
            print(rj["msg"])
    else:
        print(r.text)
        print("failed with error {r.status_code}".format(**locals()))


def start_instance(id,args):

    json_blob ={"state": "running"}
    if isinstance(id,list):
        url = apiurl(args, "/instances/")
        json_blob["ids"] = id
    else:
        url = apiurl(args, "/instances/{id}/".format(id=id))

    if (args.explain):
        print("request json: ")
        print(json_blob)
    r = http_put(args, url,  headers=headers,json=json_blob)
    r.raise_for_status()

    if (r.status_code == 200):
        rj = r.json()
        if (rj["success"]):
            print("starting instance {id}.".format(**(locals())))
        else:
            print(rj["msg"])
        return True
    else:
        print(r.text)
        print("failed with error {r.status_code}".format(**locals()))
    return False


def stop_instance(id,args):

    json_blob ={"state": "stopped"}
    if isinstance(id,list):
        url = apiurl(args, "/instances/")
        json_blob["ids"] = id
    else:
        url = apiurl(args, "/instances/{id}/".format(id=id))

    if (args.explain):
        print("request json: ")
        print(json_blob)
    r = http_put(args, url,  headers=headers,json=json_blob)
    r.raise_for_status()

    if (r.status_code == 200):
        rj = r.json()
        if (rj["success"]):
            print("stopping instance {id}.".format(**(locals())))
        else:
            print(rj["msg"])
        return True
    else:
        print(r.text)
        print("failed with error {r.status_code}".format(**locals()))
    return False


def destroy_instance_silent(id, args):
    """
    Silently destroys a specified instance, retrying up to three times if it fails.

    This function calls the `destroy_instance` function to terminate an instance.
    If the `args.raw` flag is set to True, the output of the destruction process
    is suppressed to keep the console output clean.

    Args:
        id (str): The ID of the instance to destroy.
        args (argparse.Namespace): Command-line arguments containing necessary flags.

    Returns:
        dict: A dictionary with a success status and error message, if any.
    """
    max_retries = 10
    for attempt in range(1, max_retries + 1):
        try:
            # Suppress output if args.raw is True
            if args.raw:
                with open(os.devnull, 'w') as devnull, redirect_stdout(devnull), redirect_stderr(devnull):
                    destroy_instance(id, args)
            else:
                destroy_instance(id, args)

            # If successful, exit the loop and return success
            if not args.raw:
                print(f"Instance {id} destroyed successfully on attempt {attempt}.")
            return {"success": True}

        except Exception as e:
            if not args.raw:
                print(f"Error destroying instance {id}: {e}")

        # Wait before retrying if the attempt failed
        if attempt < max_retries:
            if not args.raw:
                print(f"Retrying in 10 seconds... (Attempt {attempt}/{max_retries})")
            time.sleep(10)
        else:
            if not args.raw:
                print(f"Failed to destroy instance {id} after {max_retries} attempts.")
            return {"success": False, "error": "Max retries exceeded"}


##################################################################################################
# Search & Offer Helpers
##################################################################################################
def fetch_url_content(url):
    response = requests.get(url)
    response.raise_for_status()  # Raises an HTTPError for bad responses
    return response.text


def get_gpu_names() -> List[str]:
    """Returns a set of GPU names available on Vast.ai, with results cached for 24 hours."""
    
    def is_cache_valid() -> bool:
        """Checks if the cache file exists and is less than 24 hours old."""
        if not os.path.exists(CACHE_FILE):
            return False
        cache_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(CACHE_FILE))
        return cache_age < CACHE_DURATION
    
    if is_cache_valid():
        with open(CACHE_FILE, "r") as file:
            gpu_names = json.load(file)
    else:
        endpoint = "/api/v0/gpu_names/unique/"
        url = f"{server_url_default}{endpoint}"
        r = requests.get(url, headers={})
        r.raise_for_status()  # Will raise an exception for HTTP errors
        gpu_names = r.json()
        with open(CACHE_FILE, "w") as file:
            json.dump(gpu_names, file)

    formatted_gpu_names = [
        name.replace(" ", "_").replace("-", "_") for name in gpu_names['gpu_names']
    ]
    return formatted_gpu_names


REGIONS = {
"North_America": "[AG, BS, BB, BZ, CA, CR, CU, DM, DO, SV, GD, GT, HT, HN, JM, MX, NI, PA, KN, LC, VC, TT, US]",
"South_America": "[AR, BO, BR, CL, CO, EC, FK, GF, GY, PY, PE, SR, UY, VE]",
"Europe": "[AL, AD, AT, BY, BE, BA, BG, HR, CY, CZ, DK, EE, FI, FR, DE, GR, HU, IS, IE, IT, LV, LI, LT, LU, MT, MD, MC, ME, NL, MK, NO, PL, PT, RO, RU, SM, RS, SK, SI, ES, SE, CH, UA, GB, VA, XK]",
"Asia": "[AF, AM, AZ, BH, BD, BT, BN, KH, CN, GE, IN, ID, IR, IQ, IL, JP, JO, KZ, KW, KG, LA, LB, MY, MV, MN, MM, NP, KP, OM, PK, PH, QA, SA, SG, KR, LK, SY, TW, TJ, TH, TL, TR, TM, AE, UZ, VN, YE, HK, MO]",
"Oceania": "[AS, AU, CK, FJ, PF, GU, KI, MH, FM, NR, NC, NZ, NU, MP, PW, PG, PN, WS, SB, TK, TO, TV, VU, WF]",
"Africa": "[DZ, AO, BJ, BW, BF, BI, CV, CM, CF, TD, KM, CG, CD, CI, DJ, EG, GQ, ER, SZ, ET, GA, GM, GH, GN, GW, KE, LS, LR, LY, MG, MW, ML, MR, MU, MA, MZ, NA, NE, NG, RW, ST, SN, SC, SL, SO, ZA, SS, SD, TZ, TG, TN, UG, ZM, ZW]"
}


def is_valid_region(region):
    """region is valid if it is a key in REGIONS or a string list of country codes."""
    if region in REGIONS:
        return True
    if region.startswith("[") and region.endswith("]"):
        country_codes = region[1:-1].split(',')
        return all(len(code.strip()) == 2 for code in country_codes)
    return False


def parse_region(region):
    """Returns a string in a list format of two-char country codes."""
    if region in REGIONS:
        return REGIONS[region]
    return region


def numeric_version(version_str):
    try:
        # Split the version string by the period
        major, minor, patch = version_str.split('.')

        # Pad each part with leading zeros to make it 3 digits
        major = major.zfill(3)
        minor = minor.zfill(3)
        patch = patch.zfill(3)

        # Concatenate the padded parts
        numeric_version_str = f"{major}{minor}{patch}"

        # Convert the concatenated string to an integer
        result = int(numeric_version_str)
        #print(result)
        return result

    except ValueError:
        print("Invalid version string format. Expected format: X.X.X")
        return None


##################################################################################################
# Threading Utilities
##################################################################################################
def exec_with_threads(f, args, nt=16, max_retries=5):
    def worker(sub_args):
        for arg in sub_args:
            retries = 0
            while retries <= max_retries:
                try:
                    result = None
                    if isinstance(arg,tuple):
                        result = f(*arg)
                    else:
                        result = f(arg)
                    if result:  # Assuming a truthy return value means success
                        break
                except Exception as e:
                    print(str(e))
                    pass
                retries += 1
                stime = 0.25 * 1.3 ** retries
                print(f"retrying in {stime}s")
                time.sleep(stime)  # Exponential backoff

    # Split args into nt sublists
    args_per_thread = math.ceil(len(args) / nt)
    sublists = [args[i:i + args_per_thread] for i in range(0, len(args), args_per_thread)]

    with ThreadPoolExecutor(max_workers=nt) as executor:
        executor.map(worker, sublists)


def split_into_sublists(lst, k):
    # Calculate the size of each sublist
    sublist_size = (len(lst) + k - 1) // k
    
    # Create the sublists using list comprehension
    sublists = [lst[i:i + sublist_size] for i in range(0, len(lst), sublist_size)]
    
    return sublists


def split_list(lst, k):
    """
    Splits a list into sublists of maximum size k.
    """
    return [lst[i:i + k] for i in range(0, len(lst), k)]


##################################################################################################
# Billing & Invoices Helpers
##################################################################################################
def inv_sum(X, k):
    y = 0
    for x in X:
        a = float(x.get(k,0))
        y += a
    return y


def select(X,k):
    Y = set()
    for x in X:
        v = x.get(k,None)
        if v is not None:
            Y.add(v)
    return Y
# Helper to convert date string or int to timestamp


def to_timestamp_(val):
    if isinstance(val, int):
        return val
    if isinstance(val, str):
        if val.isdigit():
            return int(val)
        return int(datetime.strptime(val + "+0000", '%Y-%m-%d%z').timestamp())
    raise ValueError("Invalid date format")

charge_types = ['instance','volume','serverless', 'i', 'v', 's']
invoice_types = {
    "transfers": "transfer",
    "stripe": "stripe_payments",
    "bitpay": "bitpay",
    "coinbase": "coinbase",
    "crypto.com": "crypto.com",
    "reserved": "instance_prepay",
    "payout_paypal": "paypal_manual",
    "payout_wise": "wise_manual"
}


def filter_invoice_items(args: argparse.Namespace, rows: List) -> Dict:
    """This applies various filters to the invoice items. Currently it filters on start and end date and applies the
    'only_charge' and 'only_credits' options.invoice_number

    :param argparse.Namespace args: should supply all the command-line options
    :param List rows: The rows of items in the invoice

    :rtype List: Returns the filtered list of rows.

    """

    try:
        #import vast_pdf
        import dateutil
        from dateutil import parser
    except ImportError:
        print("""\nWARNING: The 'vast_pdf' library is not present. This library is used to print invoices in PDF format. If
        you do not need this feature you can ignore this message. To get the library you should download the vast-python
        github repository. Just do 'git@github.com:vast-ai/vast-python.git' and then 'cd vast-python'. Once in that
        directory you can run 'vast.py' and it will have access to 'vast_pdf.py'. The library depends on a Python
        package called Borb to make the PDF files. To install this package do 'pip3 install borb'.\n""")

    """
    try:
        vast_pdf
    except NameError:
        vast_pdf = Object()
        vast_pdf.invoice_number = -1
    """

    selector_flag = ""
    end_timestamp: float = 9999999999
    start_timestamp: float = 0
    start_date_txt = ""
    end_date_txt = ""

    if args.end_date:
        try:
            end_date = dateutil.parser.parse(str(args.end_date))
            end_date_txt = end_date.isoformat()
            end_timestamp = time.mktime(end_date.timetuple())
        except ValueError:
            print("Warning: Invalid end date format! Ignoring end date!")
    if args.start_date:
        try:
            start_date = dateutil.parser.parse(str(args.start_date))
            start_date_txt = start_date.isoformat()
            start_timestamp = time.mktime(start_date.timetuple())
        except ValueError:
            print("Warning: Invalid start date format! Ignoring start date!")

    if args.only_charges:
        type_txt = "Only showing charges."
        selector_flag = "only_charges"

        def type_filter_fn(row):
            return True if row["type"] == "charge" else False
    elif args.only_credits:
        type_txt = "Only showing credits."
        selector_flag = "only_credits"

        def type_filter_fn(row):
            return True if row["type"] == "payment" else False
    else:
        type_txt = ""

        def type_filter_fn(row):
            return True

    if args.end_date:
        if args.start_date:
            header_text = f'Invoice items after {start_date_txt} and before {end_date_txt}.'
        else:
            header_text = f'Invoice items before {end_date_txt}.'
    elif args.start_date:
        header_text = f'Invoice items after {start_date_txt}.'
    else:
        header_text = " "

    header_text = header_text + " " + type_txt

    rows = list(filter(lambda row: end_timestamp >= (row["timestamp"] or 0.0) >= start_timestamp and type_filter_fn(row) and float(row["amount"]) != 0, rows))

    if start_date_txt:
        start_date_txt = "S:" + start_date_txt

    if end_date_txt:
        end_date_txt = "E:" + end_date_txt

    now = date.today()
    invoice_number: int = now.year * 12 + now.month - 1


    pdf_filename_fields = list(filter(lambda fld: False if fld == "" else True,
                                      [str(invoice_number),
                                       start_date_txt,
                                       end_date_txt,
                                       selector_flag]))

    filename = "invoice_" + "-".join(pdf_filename_fields) + ".pdf"
    return {"rows": rows, "header_text": header_text, "pdf_filename": filename}


##################################################################################################
# Host / Machine Helpers
##################################################################################################
def cleanup_machine(args, machine_id):
    req_url = apiurl(args, f"/machines/{machine_id}/cleanup/")

    if (args.explain):
        print("request json: ")
    r = http_put(args, req_url, headers=headers, json={})

    if (r.status_code == 200):
        rj = r.json()
        if (rj["success"]):
            print(json.dumps(r.json(), indent=1))
        else:
            if args.raw:
                return r
            else:
                print(rj["msg"])
    else:
        print(r.text)
        print("failed with error {r.status_code}".format(**locals()))


def list_machine(args, id):
    req_url = apiurl(args, "/machines/create_asks/")

    json_blob = {
        'machine': id,
        'price_gpu': args.price_gpu,
        'price_disk': args.price_disk,
        'price_inetu': args.price_inetu,
        'price_inetd': args.price_inetd,
        'price_min_bid': args.price_min_bid,
        'min_chunk': args.min_chunk,
        'end_date': string_to_unix_epoch(args.end_date),
        'credit_discount_max': args.discount_rate,
        'duration': args.duration,
        'vol_size': args.vol_size,
        'vol_price': args.vol_price
    }
    if (args.explain):
        print("request json: ")
        print(json_blob)
    r = http_put(args, req_url, headers=headers, json=json_blob)

    if (r.status_code == 200):
        rj = r.json()
        if (rj["success"]):
            price_gpu_ = str(args.price_gpu) if args.price_gpu is not None else "def"
            price_inetu_ = str(args.price_inetu)
            price_inetd_ = str(args.price_inetd)
            min_chunk_ = str(args.min_chunk)
            end_date_ = string_to_unix_epoch(args.end_date)
            discount_rate_ = str(args.discount_rate)
            duration_ = str(args.duration)
            if args.raw:
                return r
            else:
                print("offers created/updated for machine {id},  @ ${price_gpu_}/gpu/hr, ${price_inetu_}/GB up, ${price_inetd_}/GB down, {min_chunk_}/min gpus, max discount_rate {discount_rate_}, till {end_date_}, duration {duration_}".format(**locals()))
                num_extended = rj.get("extended", 0)

                if num_extended > 0:
                    print(f"extended {num_extended} client contracts to {args.end_date}")

        else:
            if args.raw:
                return r
            else:
                print(rj["msg"])
    else:
        print(r.text)
        print("failed with error {r.status_code}".format(**locals()))


def set_ask(args):
    """

    :param argparse.Namespace args: should supply all the command-line options
    :rtype:
    """
    print("set asks!\n")


##################################################################################################
# Environment Parsing
##################################################################################################
def smart_split(s, char):
    in_double_quotes = False
    in_single_quotes = False #note that isn't designed to work with nested quotes within the env
    parts = []
    current = []

    for c in s:
        if c == char and not (in_double_quotes or in_single_quotes):
            parts.append(''.join(current))
            current = []
        elif c == '\'':
            in_single_quotes = not in_single_quotes
            current.append(c)
        elif c == '\"':
            in_double_quotes = not in_double_quotes
            current.append(c)
        else:
            current.append(c)
    parts.append(''.join(current))  # add last part
    return parts


def parse_env(envs):
    result = {}
    if (envs is None):
        return result
    env = smart_split(envs,' ')
    prev = None
    for e in env:
        if (prev is None):
          if (e in {"-e", "-p", "-h", "-v", "-n"}):
              prev = e
          else:
            pass
        else:
          if (prev == "-p"):
            if set(e).issubset(set("0123456789:tcp/udp")):
                result["-p " + e] = "1"
            else:
                pass
          elif (prev == "-e"):
            kv = e.split('=')
            if len(kv) >= 2: #set(e).issubset(set("1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_=")):
                val = kv[1]
                if len(kv) > 2:
                    val = '='.join(kv[1:])
                result[kv[0]] = val.strip("'\"")
            else:
                pass
          elif (prev == "-v"):
            if (set(e).issubset(set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:./_"))):
                result["-v " + e] = "1" 
          elif (prev == "-n"):
            if (set(e).issubset(set("abcdefghijklmnopqrstuvwxyz0123456789-"))):
                result["-n " + e] = "1"
          else:
              result[prev] = e
          prev = None
    #print(result)
    return result


#print(parse_env("-e TYZ=BM3828 -e BOB=UTC -p 10831:22 -p 8080:8080"))


def pretty_print_POST(req):
    print('{}\n{}\r\n{}\r\n\r\n{}'.format(
        '-----------START-----------',
        req.method + ' ' + req.url,
        '\r\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
        req.body,
    ))


##################################################################################################
# TFA (Two-Factor Authentication) Helpers
##################################################################################################
def display_tfa_methods(methods):
    """Helper function to display 2FA methods in a table."""
    method_fields = TFA_METHOD_FIELDS
    has_sms = any(m['method'] == 'sms' for m in methods)
    if not has_sms:  # Don't show Phone Number column if the user has no SMS methods
        method_fields = tuple(field for field in TFA_METHOD_FIELDS if field[0] != 'phone_number')
    
    display_table(methods, method_fields, replace_spaces=False)


def handle_failed_tfa_verification(args, e):
    error_data = e.response.json()
    error_msg = error_data.get("msg", str(e))
    error_code = error_data.get("error", "")

    if args.raw:
        print(json.dumps(error_data, indent=2))
    
    print(f"\n{FAIL} Error: {error_msg}")
    
    # Provide helpful context for common errors
    if error_code in {"tfa_locked", "2fa_verification_failed"}:
        fail_count = error_data.get("fail_count", 0)
        locked_until = error_data.get("locked_until")
        
        if fail_count > 0:
            print(f"   Failed attempts: {fail_count}")
        if locked_until:
            lock_time_sec = (datetime.fromtimestamp(locked_until) - datetime.now()).seconds
            minutes, seconds = divmod(lock_time_sec, 60)
            print(f"   Time Remaining for 2FA Lock: {minutes} minutes and {seconds} seconds...")

    elif error_code == "2fa_expired":
        # Note: Only SMS & email use tfa challenges that expire when verifying
        print(f"\n   The {args.method_type} 2FA code and secret have expired. Please start over:")
        print(f"     vastai tfa send-{args.method_type}")


def format_backup_codes(backup_codes):
    """Format backup codes for display or file output."""
    output_lines = [
        "=" * 60, "  VAST.AI 2FA BACKUP CODES", "=" * 60,
        f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"\n{WARN}  WARNING: All previous backup codes are now invalid!",
        "\nYour New Backup Codes (one-time use only):",
        "-" * 40,
    ]
    
    for i, code in enumerate(backup_codes, 1):
        output_lines.append(f"  {i:2d}. {code}")
    
    output_lines.extend([
        "-" * 40,
        "\nIMPORTANT:",
        " • Each code can only be used once",
        " • Store them in a secure location",
        " • Use these codes to log in if you lose access to your 2FA device",
        "\n" + "=" * 60,
    ])
    return "\n".join(output_lines)


def confirm_destructive_action(prompt="Are you sure? (y/n): "):
    """Prompt user for confirmation of destructive actions"""
    try:
        response = input(f" {prompt}").strip().lower()
        return 'y' in response
    except (EOFError, KeyboardInterrupt):
        print("\nOperation cancelled.")
        raise


def save_to_file(content, filepath):
    """Save content to file, creating parent directories if needed."""
    try:
        filepath = os.path.abspath(os.path.expanduser(filepath))
        
        # If directory provided, this should be handled by caller
        parent_dir = os.path.dirname(filepath)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        
        with open(filepath, "w") as f:
            f.write(content)
        return True
    except (IOError, OSError) as e:
        print(f"\n{FAIL} Error saving file: {e}")
        return False


def get_backup_codes_filename():
    """Generate a timestamped filename for backup codes."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"vastai_backup_codes_{timestamp}.txt"


def save_backup_codes(backup_codes):
    """Save or display 2FA backup codes based on user choice."""
    print(f"\nBackup codes regenerated successfully! {SUCCESS}")
    print(f"\n{WARN}  WARNING: All previous backup codes are now invalid!")
    
    formatted_content = format_backup_codes(backup_codes)
    filename = get_backup_codes_filename()
    
    while True:
        print("\nHow would you like to save your new backup codes?")
        print(f"  1. Save to default location (~/Downloads/{filename})")
        print(f"  2. Save to a custom path")
        print(f"  3. Print to screen ({WARN}  potentially unsafe - visible to onlookers)")
        
        try:
            choice = input("\nEnter choice (1-3): ").strip()
            
            if choice in {'1', '2'}:
                # Determine filepath
                if choice == '1':
                    downloads_dir = os.path.expanduser("~/Downloads")
                    filepath = os.path.join(downloads_dir, filename)
                else:  # choice == '2'
                    custom_path = input("\nEnter full path for backup codes file: ").strip()
                    if not custom_path:
                        print("Error: Path cannot be empty")
                        continue
                    
                    filepath = os.path.abspath(os.path.expanduser(custom_path))
                    
                    # If directory provided, add filename
                    if os.path.isdir(filepath):
                        filepath = os.path.join(filepath, filename)
                
                # Try to save
                if save_to_file(formatted_content, filepath):
                    print(f"\n{SUCCESS} Backup codes saved to: {filepath}")
                    print(f"\nIMPORTANT:")
                    print(f" • The file contains {len(backup_codes)} one-time use backup codes")
                    if choice == '1':
                        print(f" • Move this file to a secure location")
                    return
                else:
                    print("Please try again with a different path.")
                    continue
            
            elif choice == '3':
                print(f"\n{WARN}  WARNING: Printing sensitive codes to screen!")
                confirm = input("\nAre you sure? Anyone nearby can see these codes. (yes/no): ").strip().lower()
                
                if confirm in {'yes', 'y'}:
                    print("\n" + formatted_content + "\n")
                    return
                else:
                    print("Cancelled. Please choose another option.")
                    continue
            
            else:
                print("Invalid choice. Please enter 1, 2, or 3.")
        
        except (EOFError, KeyboardInterrupt):
            print("\n\nOperation cancelled. Your backup codes were generated but not saved.")
            print("You will need to regenerate them to get new codes.")
            raise


def build_tfa_verification_payload(args, **kwargs):
    """Build common payload for TFA verification requests."""
    payload = {
        "tfa_method_id": getattr(args, 'method_id', None),
        "tfa_method": getattr(args, 'method_type', None),
        "code": getattr(args, 'code', None),
        "backup_code": getattr(args, 'backup_code', None),
        "secret": getattr(args, 'secret', None),
    }
    for key, value in kwargs.items():
        payload[key] = value

    return {k:v for k,v in payload.items() if v}


def print_next_steps_after_new_method_auth():
    print(f"\nNext Steps:"
        "\n To add a new SMS 2FA method:" 
        "\n    1. Run `vastai tfa send-sms --phone-number <PHONE_NUMBER>` to receive SMS and get secret token"
        "\n    2. Run `vastai tfa activate --method-type sms --secret <SECRET> --phone-number <PHONE_NUMBER> CODE` to activate the new method with the code you received via SMS\n"
        "\n To add a new TOTP (Authenticator app) 2FA method:"
        "\n    1. Run `vastai tfa totp-setup` to get the manual key/QR code and secret"
        "\n    2. Enter the manual key or scan the QR code with your Authenticator app"
        "\n    3. Run `vastai tfa activate --method-type totp --secret <SECRET> CODE` to activate the new method with the 6-digit code from your app")


##################################################################################################
# Self-Test & Machine Testing
##################################################################################################
def suppress_stdout():
    """
    A context manager to suppress standard output (stdout) within its block.

    This is useful for silencing output from functions or blocks of code that 
    print to stdout, especially when such output is not needed or should be 
    hidden from the user.

    Usage:
        with suppress_stdout():
            # Code block with suppressed stdout
            some_function_that_prints()

    Yields:
        None
    """
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout


def progress_print(args, *args_to_print):
    """
    Prints progress messages to the console based on the `raw` flag.

    This function ensures that progress messages are only printed when the `raw`
    output mode is not enabled. This is useful for controlling the verbosity of
    the script's output, especially in machine-readable formats.

    Args:
        args (argparse.Namespace): Parsed command-line arguments containing flags
                                  and options such as `raw`.
        *args_to_print: Variable length argument list of messages to print.

    Returns:
        None
    """
    if not args.raw:
        print(*args_to_print)


def debug_print(args, *args_to_print):
    """
    Prints debug messages to the console based on the `debugging` and `raw` flags.

    This function ensures that debug messages are only printed when debugging is
    enabled and the `raw` output mode is not active. It helps in providing detailed
    logs for troubleshooting without cluttering the standard output during normal
    operation.

    Args:
        args (argparse.Namespace): Parsed command-line arguments containing flags
                                  and options such as `debugging` and `raw`.
        *args_to_print: Variable length argument list of debug messages to print.

    Returns:
        None
    """
    if args.debugging and not args.raw:
        print(*args_to_print)


def instance_exist(instance_id, api_key, args):
    try:
        from . import vast as _vast
    except ImportError:
        import vast as _vast  # type: ignore
    show__instance = _vast.show__instance
    if not hasattr(args, 'debugging'):
        args.debugging = False

    if not instance_id:
        return False

    show_args = argparse.Namespace(
        id=instance_id,
        api_key=api_key,
        url=args.url,
        retry=args.retry,
        explain=False,
        raw=True,
        debugging=args.debugging
    )
    try:
        instance_info = show__instance(show_args)
        
        # Empty list or None means instance doesn't exist - return False without error
        if not instance_info:
            return False

        # If we have instance info, check its status
        status = instance_info.get('intended_status') or instance_info.get('actual_status')
        if status in ['destroyed', 'terminated', 'offline']:
            return False

        return True

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            # Instance does not exist
            return False
        else:
            if args.debugging:
                debug_print(args, f"HTTPError when checking instance existence: {e}")
            return False
    except Exception as e:
        if args.debugging:
            debug_print(args, f"No instance found or Unexpected error checking instance existence: {e}")
        return False


def run_machinetester(ip_address, port, instance_id, machine_id, delay, args, api_key=None):
    """
    Executes machine testing by connecting to the specified IP and port, monitoring
    the instance's status, and handling test completion or failures.

    This function performs the following steps:
        1. Disables SSL warnings.
        2. Optionally delays the start of testing.
        3. Continuously checks the instance status and attempts to connect to the
           `/progress` endpoint to monitor test progress.
        4. Handles different response messages, such as completion or errors.
        5. Implements timeout logic to prevent indefinite waiting.
        6. Ensures instance cleanup in case of failures or completion.

    Args:
        ip_address (str): The public IP address of the instance to test.
        port (int): The port number to connect to for testing.
        instance_id (str): The ID of the instance being tested.
        machine_id (str): The machine ID associated with the instance.
        delay (int): The number of seconds to delay before starting the test.
        args (argparse.Namespace): Parsed command-line arguments containing flags
                                  and options such as `debugging` and `raw`.
        api_key (str, optional): API key for authentication. Defaults to None.

    Returns:
        tuple:
            - bool: `True` if the test was successful, `False` otherwise.
            - str: Reason for failure if the test was not successful, empty string otherwise.
    """

    try:
        from . import vast as _vast
    except ImportError:
        import vast as _vast  # type: ignore
    show__instance = _vast.show__instance
    search__offers = _vast.search__offers

    # Temporarily disable SSL warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    delay = int(delay)

    # Ensure debugging is set in args
    if not hasattr(args, 'debugging'):
        args.debugging = False

    def is_instance(instance_id):
        """Check instance status via show__instance."""
        show_args = argparse.Namespace(
            id=instance_id,
            explain=False,
            api_key=api_key,
            url="https://console.vast.ai",
            retry=3,
            raw=True,
            debugging=args.debugging,
        )
        try:
            instance_info = show__instance(show_args)
            if args.debugging:
                debug_print(args, f"is_instance(): Output from vast show instance: {instance_info}")

            if not instance_info or not isinstance(instance_info, dict):
                if args.debugging:
                    debug_print(args, "is_instance(): No valid instance information received.")
                return 'unknown'

            actual_status = instance_info.get('actual_status', 'unknown')
            return actual_status if actual_status in ['running', 'offline', 'exited', 'created'] else 'unknown'
        except Exception as e:
            if args.debugging:
                debug_print(args, f"is_instance(): Error: {e}")
            return 'unknown'

    # Prepare destroy_args with required attributes set to False as needed
    destroy_args = argparse.Namespace(api_key=api_key, url="https://console.vast.ai", retry=3, explain=False, raw=args.raw, debbuging=args.debugging,)

    # Delay start if specified
    if delay > 0:
        if args.debugging:
            debug_print(args, f"Sleeping for {delay} seconds before starting tests.")
        time.sleep(delay)

    progress_print(args, f"Polling test progress at https://{ip_address}:{port}/progress every 20s (max 600s).")
    progress_print(args, f"To manually check: curl -k https://{ip_address}:{port}/progress")

    start_time = time.time()
    no_response_seconds = 0
    printed_lines = set()
    first_connection_established = False  # Flag to track first successful connection
    instance_destroyed = False  # Track whether the instance has been destroyed
    try:
        while time.time() - start_time < 600:
            # Check instance status with high priority for offline status
            status = is_instance(instance_id)
            if args.debugging:
                debug_print(args, f"Instance {instance_id} status: {status}")
                
            if status == 'offline':
                reason = "Instance offline during testing"
                progress_print(args, f"Instance {instance_id} went offline. {reason}")
                destroy_instance_silent(instance_id, destroy_args)
                instance_destroyed = True
                with open("Error_testresults.log", "a") as f:
                    f.write(f"{machine_id}:{instance_id} {reason}\n")
                return False, reason

            # Attempt to connect to the progress endpoint
            try:
                if args.debugging:
                    debug_print(args, f"Sending GET request to https://{ip_address}:{port}/progress")
                response = requests.get(f'https://{ip_address}:{port}/progress', verify=False, timeout=10)
                
                if response.status_code == 200 and not first_connection_established:
                    progress_print(args, "Successfully established HTTPS connection to the server.")
                    first_connection_established = True

                message = response.text.strip()
                if args.debugging:
                    debug_print(args, f"Received message: '{message}'")
            except requests.exceptions.RequestException as e:
                if args.debugging:
                    progress_print(args, f"Error making HTTPS request: {e}")
                message = ''

            # Process response messages
            if message:
                lines = message.split('\n')
                new_lines = [line for line in lines if line not in printed_lines]
                for line in new_lines:
                    if line == 'DONE':
                        progress_print(args, "Test completed successfully.")
                        with open("Pass_testresults.log", "a") as f:
                            f.write(f"{machine_id}\n")
                        progress_print(args, f"Test passed.")
                        destroy_instance_silent(instance_id, destroy_args)
                        instance_destroyed = True
                        return True, ""
                    elif line.startswith('ERROR'):
                        progress_print(args, line)
                        with open("Error_testresults.log", "a") as f:
                            f.write(f"{machine_id}:{instance_id} {line}\n")
                        progress_print(args, f"Test failed with error: {line}.")
                        destroy_instance_silent(instance_id, destroy_args)
                        instance_destroyed = True
                        return False, line
                    else:
                        progress_print(args, line)
                    printed_lines.add(line)
                no_response_seconds = 0
            else:
                no_response_seconds += 20
                if args.debugging:
                    debug_print(args, f"No message received. Incremented no_response_seconds to {no_response_seconds}.")

            if status == 'running' and no_response_seconds >= 120:
                if not first_connection_established:
                    reason_msg = (
                        f"Port {port} was never reachable on {ip_address}. "
                        f"The container may have crashed before the test server started, "
                        f"or port 5000 is blocked by the machine's firewall."
                    )
                    suggestions = [
                        f"  - Verify port is accessible: curl -k https://{ip_address}:{port}/progress",
                        f"  - Check direct_port_count: vastai search offers 'machine_id={machine_id} rentable=any verified=any'",
                        f"  - The container may have crashed on startup (CUDA/driver error). Ask the host to check: docker logs <container>",
                        f"  - Check nvidia-smi is working on the host machine.",
                    ]
                    return_reason = f"Port {port} unreachable — container startup failure or port misconfiguration."
                else:
                    reason_msg = (
                        f"Connection to port {port} on {ip_address} was established but then lost. "
                        f"The container likely crashed mid-test (OOM, GPU error, or a failing test)."
                    )
                    suggestions = [
                        f"  - Ask the host to check docker logs for the container.",
                        f"  - Check available system RAM vs GPU VRAM (OOM risk).",
                        f"  - Check nvidia-smi for GPU errors on the host.",
                        f"  - Run tests individually to isolate which test caused the crash.",
                    ]
                    return_reason = "Connection lost mid-test — likely OOM, GPU error, or crash during tests."
                with open("Error_testresults.log", "a") as f:
                    f.write(f"{machine_id}:{instance_id} {return_reason} (first_connection={first_connection_established}, port={port}, ip={ip_address})\n")
                progress_print(args, f"No response for 120s with running instance. {reason_msg}")
                progress_print(args, f"Suggestions to investigate:")
                for s in suggestions:
                    progress_print(args, s)
                destroy_instance_silent(instance_id, destroy_args)
                instance_destroyed = True
                return False, return_reason

            if args.debugging:
                debug_print(args, "Waiting for 20 seconds before the next check.")
            time.sleep(20)

        if args.debugging:
            debug_print(args, f"Time limit reached. Destroying instance {instance_id}.")
        return False, "Test did not complete within the time limit"
    finally:
        # Ensure instance cleanup
        if not instance_destroyed and instance_id and instance_exist(instance_id, api_key, destroy_args):
           destroy_instance_silent(instance_id, destroy_args)
        progress_print(args, f"Machine: {machine_id} Done with testing remote.py results {message}")
        warnings.simplefilter('default')


def safe_float(value):
    """
    Convert value to float, returning 0 if value is None.
    
    Args:
        value: The value to convert to float
        
    Returns:
        float: The converted value, or 0 if value is None
    """
    if value is None:
        return 0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0


def check_requirements(machine_id, api_key, args):
    """
    Validates whether a machine meets the specified hardware and performance requirements.

    This function queries the machine's offers and checks various criteria such as CUDA
    version, reliability, port count, PCIe bandwidth, internet speeds, GPU RAM, system
    RAM, and CPU cores relative to the number of GPUs. If any of these requirements are
    not met, it records the reasons for the failure.

    Args:
        machine_id (str): The ID of the machine to check.
        api_key (str): API key for authentication with the VAST API.
        args (argparse.Namespace): Parsed command-line arguments containing flags
                                  and options such as `debugging` and `raw`.

    Returns:
        tuple:
            - bool: `True` if the machine meets all requirements, `False` otherwise.
            - list: A list of reasons why the machine does not meet the requirements.
    """
    try:
        from . import vast as _vast
    except ImportError:
        import vast as _vast  # type: ignore

    unmet_reasons = []

    # Prepare search arguments to get machine offers
    search_args = argparse.Namespace(
        query=[f"machine_id={machine_id}", "verified=any", "rentable=true", "rented=any"],
        type="on-demand",
        quiet=False,
        no_default=False,
        new=False,
        limit=None,
        storage=5.0,
        order="score-",
        raw=True,  # Ensure raw output to get data directly
        explain=args.explain,
        api_key=api_key,
        url=args.url,
        retry=args.retry
    )

    try:
        # Call search__offers and capture the return value directly
        offers = _vast.search__offers(search_args)
        if args.debugging:
            debug_print(args, "Captured offers from search__offers:", offers)

        if not offers:
            unmet_reasons.append(f"Machine ID {machine_id} not found or not rentable.")
            progress_print(args, f"Machine ID {machine_id} not found or not rentable.")
            progress_print(args, f"Possible reasons and how to investigate:")
            progress_print(args, f"  1. Already rented — the machine has no remaining capacity.")
            progress_print(args, f"     Check: vastai search offers 'machine_id={machine_id} rented=true rentable=any verified=any'")
            progress_print(args, f"  2. Machine went offline — it may have disconnected since you last checked.")
            progress_print(args, f"     Check: vastai show machines  (look for machine {machine_id} and its status)")
            progress_print(args, f"  3. No active offer / not configured as rentable — the host may not have listed this machine.")
            progress_print(args, f"     Check: vastai search offers 'machine_id={machine_id} rentable=any rented=any verified=any'")
            progress_print(args, f"  4. Bid price below ask — your bid may be lower than the host's minimum price.")
            progress_print(args, f"     Check: vastai search offers 'machine_id={machine_id} rentable=any verified=any' and compare prices.")
            return False, unmet_reasons

        # Sort offers based on 'dlperf' in descending order
        sorted_offers = sorted(offers, key=lambda x: x.get('dlperf', 0), reverse=True)
        top_offer = sorted_offers[0]

        if args.debugging:
            debug_print(args, "Top offer found:", top_offer)

        # Requirement checks
        # 1. CUDA version
        if safe_float(top_offer.get('cuda_max_good')) < 11.8:
            unmet_reasons.append("CUDA version < 11.8")

        # 2. Reliability
        if safe_float(top_offer.get('reliability')) <= 0.90:
            unmet_reasons.append("Reliability <= 0.90")

        # 3. Direct port count
        if safe_float(top_offer.get('direct_port_count')) <= 3:
            unmet_reasons.append("Direct port count <= 3")

        # 4. PCIe bandwidth
        if safe_float(top_offer.get('pcie_bw')) <= 2.85:
            unmet_reasons.append("PCIe bandwidth <= 2.85")

        # 5. Download speed
        if safe_float(top_offer.get('inet_down')) < 500:
            unmet_reasons.append("Download speed < 500 Mb/s")

        # 6. Upload speed
        if safe_float(top_offer.get('inet_up')) < 500:
            unmet_reasons.append("Upload speed < 500 Mb/s")

        # 7. GPU RAM
        if safe_float(top_offer.get('gpu_ram')) <= 7:
            unmet_reasons.append("GPU RAM <= 7 GB")

        # Additional Requirement Checks

        # 8. System RAM vs. Total GPU RAM
        gpu_total_ram = safe_float(top_offer.get('gpu_total_ram'))  # in MB
        cpu_ram = safe_float(top_offer.get('cpu_ram'))  # in MB
        if cpu_ram < .95*gpu_total_ram: # .95 to allow for reserved hardware memory
            unmet_reasons.append("System RAM is less than total VRAM.")

        # Debugging Information for RAM
        if args.debugging:
            debug_print(args, f"CPU RAM: {cpu_ram} MB")
            debug_print(args, f"Total GPU RAM: {gpu_total_ram} MB")

        # 9. CPU Cores vs. Number of GPUs
        cpu_cores = int(safe_float(top_offer.get('cpu_cores')))
        num_gpus = int(safe_float(top_offer.get('num_gpus')))
        if cpu_cores < 2 * num_gpus:
            unmet_reasons.append("Number of CPU cores is less than twice the number of GPUs.")

        # Debugging Information for CPU Cores
        if args.debugging:
            debug_print(args, f"CPU Cores: {cpu_cores}")
            debug_print(args, f"Number of GPUs: {num_gpus}")

        # Return True if all requirements are met, False otherwise
        if unmet_reasons:
            progress_print(args, f"Machine ID {machine_id} does not meet the requirements:")
            for reason in unmet_reasons:
                progress_print(args, f"- {reason}")
            return False, unmet_reasons
        else:
            progress_print(args, f"Machine ID {machine_id} meets all the requirements.")
            return True, []

    except Exception as e:
        progress_print(args, f"An unexpected error occurred: {str(e)}")
        if args.debugging:
            debug_print(args, f"Exception details: {e}")
        return False, [f"Unexpected error: {str(e)}"]


def wait_for_instance(instance_id, api_key, args, destroy_args, timeout=900, interval=10):
    """
    Waits for an instance to reach a running state and monitors its status for errors.

    """

    try:
        from . import vast as _vast
    except ImportError:
        import vast as _vast  # type: ignore
    show__instance = _vast.show__instance

    if not hasattr(args, 'debugging'):
        args.debugging = False

    start_time = time.time()
    show_args = argparse.Namespace(
        id=instance_id,
        quiet=False,
        raw=True,  # Ensure raw output to get data directly
        explain=args.explain,
        api_key=api_key,
        url=args.url,
        retry=args.retry,
        debugging=args.debugging,
    )
    
    if args.debugging:
        debug_print(args, "Starting wait_for_instance with ID:", instance_id)
    
    while time.time() - start_time < timeout:
        try:
            # Directly call show__instance and capture the return value
            instance_info = show__instance(show_args)
            
            if not instance_info:
                progress_print(args, f"No information returned for instance {instance_id}. Retrying...")
                time.sleep(interval)
                continue  # Retry

            # Check for error in status_msg
            status_msg = instance_info.get('status_msg', '')
            if status_msg and 'Error' in status_msg:
                reason = f"Instance {instance_id} encountered an error: {status_msg.strip()}"
                progress_print(args, reason)
                
                # Destroy the instance
                if instance_exist(instance_id, api_key, destroy_args):
                    destroy_instance_silent(instance_id, destroy_args)
                    progress_print(args, f"Instance {instance_id} has been destroyed due to error.")
                else:
                    progress_print(args, f"Instance {instance_id} could not be destroyed or does not exist.")
                
                return False, reason
            
            # Check if instance went offline
            actual_status = instance_info.get('actual_status', 'unknown')
            if actual_status == 'offline':
                reason = "Instance offline during testing"
                progress_print(args, reason)
                
                # Destroy the instance
                if instance_exist(instance_id, api_key, destroy_args):
                    destroy_instance_silent(instance_id, destroy_args)
                    progress_print(args, f"Instance {instance_id} has been destroyed due to being offline.")
                else:
                    progress_print(args, f"Instance {instance_id} could not be destroyed or does not exist.")
                
                return False, reason
            
            # Check if instance is running
            if instance_info.get('intended_status') == 'running' and actual_status == 'running':
                if args.debugging:
                    debug_print(args, f"Instance {instance_id} is now running.")
                return instance_info, None  # Return instance_info with None for reason
            
            # Print feedback about the current status
            progress_print(args, f"Instance {instance_id} status: {actual_status}... waiting for 'running' status.")
            time.sleep(interval)
        
        except Exception as e:
            progress_print(args, f"Error retrieving instance info for {instance_id}: {e}. Retrying...")
            if args.debugging:
                debug_print(args, f"Exception details: {str(e)}")
            time.sleep(interval)
    
    # Timeout reached without instance running
    reason = f"Instance did not become running within {timeout} seconds. Verify network configuration. Use the self-test machine function in vast cli"
    progress_print(args, reason)
    return False, reason


##################################################################################################
# Deprecated
##################################################################################################
login_deprecated_message = """


login via the command line is no longer supported.
go to https://console.vast.ai/cli in a web browser to get your api key, then run:

    vast set api-key YOUR_API_KEY_HERE
"""
