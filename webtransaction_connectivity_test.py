"""
This script is used to test connectivity to Google PubSub.

Steps to use:
1. Place this script at: $SPLUNKHOME/bin/etc/apps/TA-NetskopeAppForSplunk/bin/ta_netskopeappforsplunk/google_pubsublite_sdk/
2. Navigate to $SPLUNKHOME/bin/
3. Run the following command to execute this script:
    ./splunk cmd python3 ../etc/apps/TA-NetskopeAppForSplunk/bin/ta_netskopeappforsplunk/google_pubsublite_sdk/webtransaction_connectivity_test.py
"""

import concurrent.futures
import json
import os
import signal
import sys
import threading
import requests
from getpass import getpass
import signal
from google.api_core.exceptions import Unauthenticated
from concurrent.futures._base import TimeoutError

TIME_TO_STOP = 300  # define time in seconds


def handler(signal, frame):
    raise Exception("Time's up!")


signal.signal(
    signal.SIGALRM, handler
)  # signal function to terminate code after TIME_TO_STOP seconds
signal.alarm(TIME_TO_STOP)

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            __file__,
            "..",
            "..",
            "netskope_iterator_sdk",
        )
    ),
)

from requests.exceptions import RequestException
from netskope_api.iterator.const import Const
from netskope_api.token_management.netskope_management import NetskopeTokenManagement

import logging

logging.basicConfig(
    filename="Webtransaction_connectivity.log",
    format="%(asctime)s %(message)s",
    filemode="w",
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


MIN_OUT_MESSAGES = 1  # Must be greater than 0
MIN_OUT_BYTES = (
    3.5 * 1024 * 1024
)  # Must be greater than the allowed size of the largest message. 3.5 MiB
MIN_THREAD_COUNT = 1
MIN_MERGED_FILESIZE_LIMIT = 3.5 * 1024 * 1024  # 3.5 MiB
MIN_CLOSE_FILE_IN_SECONDS = 10
MIN_IDLE_CONNECTION_TIMEOUT = 300
MIN_PARALLEL_INGESTION_PIPELINE = 1
MAX_MSG_WAIT_TIME_SECONDS = 3600
MIN_WAIT_TIME_TO_TERMINATE_THREAD = 3
MAX_SUB_THREADS = 3

CLOSE_FILE_IN_SECONDS = None
THREAD_COUNT = None
MERGED_FILESIZE_LIMIT = None
BYTES_OUTSTANDING = None
MESSAGE_OUTSTANDING = None
thread_lock = threading.Lock()
queue_reference = {}
queue_size = 10000
stop_flag = False

LOCAL_STORAGE_PATH = os.path.abspath(os.path.join(__file__, "..", "test_webtxn_files"))
if not os.path.exists(LOCAL_STORAGE_PATH):
    os.makedirs(LOCAL_STORAGE_PATH)


class FileTooOldException(Exception):
    pass


class FileSizeExceededException(Exception):
    pass


def create_uri(proxy_settings):
    """
    Create proxy url from the given proxy settings.

    :param proxy_enabled: True if Proxy config is enabled. False otherwise
    :param proxy_settings: Proxy metadata

    :return: Proxy URI
    """
    uri = None
    if proxy_settings.get("proxy_url"):
        uri = proxy_settings["proxy_url"]
        if proxy_settings.get("proxy_port"):
            uri = "{}:{}".format(uri, proxy_settings.get("proxy_port"))
        if proxy_settings.get("proxy_username") and proxy_settings.get("proxy_password"):
            uri = "{}://{}:{}@{}/".format(
                proxy_settings["proxy_type"],
                requests.compat.quote_plus(str(proxy_settings["proxy_username"])),
                requests.compat.quote_plus(str(proxy_settings["proxy_password"])),
                uri,
            )
        else:
            uri = "{}://{}".format(proxy_settings["proxy_type"], uri)
    return uri


def create_requests_proxies_helper(proxy_settings):
    """
    Create proxy dictionary used in requests module.

    :param proxy_enabled: True if Proxy config is enabled. False otherwise
    :param proxy_settings: Proxy metadata

    :return: Proxy dict
    """
    proxies = {}
    proxy_uri = create_uri(proxy_settings)
    if proxy_uri:
        proxies = {"http": proxy_uri, "https": proxy_uri}
    return proxies


def callback(message):
    """
    Callback method for pubsublite
    """
    if message:
        logger.info("Successfully got the message")
        print("Successfully got the message")
        os._exit(0)


def make_default_thread_pool_executor(thread_count=None):
    # Python 2.7 and 3.6+ have the thread_name_prefix argument, which is useful
    # for debugging.
    executor_kwargs = {}
    if sys.version_info[:2] == (2, 7) or sys.version_info[:2] >= (3, 6):
        executor_kwargs["thread_name_prefix"] = "CallbackThread"
    return concurrent.futures.ThreadPoolExecutor(max_workers=thread_count, **executor_kwargs)


def stream(subscription_key, subscription, proxies=None, timeout=None):
    """Connect to the gcp and fetches the messages."""
    try:
        from google.cloud.pubsublite.cloudpubsub import SubscriberClient
        from google.cloud.pubsublite.types import FlowControlSettings
        from google.oauth2 import service_account
    except ImportError as e:
        logger.info(
            "Due to 3rd party library limitation, input configuration only supports the latest version of OS: Ubuntu, CentOS, RHEL and Windows. ImportError: {}".format(
                str(e)
            )
        )
    except Exception as e:
        print(e)

    global streaming_pull_future
    global CLOSE_FILE_IN_SECONDS
    global MESSAGE_OUTSTANDING
    global BYTES_OUTSTANDING
    global BYTES_OUTSTANDING
    global MERGED_FILESIZE_LIMIT
    service_account_info = subscription_key
    additional_params = {
        "messages_outstanding": "1",
        "bytes_outstanding": "15 * 1024 * 1024",
        "thread_count": "5",
        "merged_filesize_limit": "10 * 1024 * 1024",
        "close_file_in_seconds": "30",
    }

    # Create Credential object
    service_account_info = json.loads(service_account_info)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)

    messages_outstanding = additional_params.get("messages_outstanding")
    MESSAGE_OUTSTANDING = int(eval(messages_outstanding))

    bytes_outstanding = additional_params.get("bytes_outstanding")
    BYTES_OUTSTANDING = int(eval(bytes_outstanding))

    thread_count = additional_params.get("thread_count")
    THREAD_COUNT = int(eval(thread_count))

    close_file_in_seconds = additional_params.get("close_file_in_seconds")
    CLOSE_FILE_IN_SECONDS = int(eval(close_file_in_seconds))

    merged_filesize_limit = additional_params.get("merged_filesize_limit")
    MERGED_FILESIZE_LIMIT = int(eval(merged_filesize_limit))

    per_partition_flow_control_settings = FlowControlSettings(
        # Must be >0.
        messages_outstanding=MESSAGE_OUTSTANDING,
        # Must be greater than the allowed size of the largest message.
        bytes_outstanding=BYTES_OUTSTANDING,
    )
    executor = make_default_thread_pool_executor(thread_count=THREAD_COUNT)

    # Setup proxy
    if proxies:
        # Splunk's local network calls throws error if NO_PROXY is not set.
        os.environ["no_proxy"] = "localhost,127.0.0.1,0.0.0.0,localaddress"
        os.environ["NO_PROXY"] = "localhost,127.0.0.1,0.0.0.0,localaddress"

        os.environ["http_proxy"] = proxies.get("http")
        os.environ["HTTP_PROXY"] = proxies.get("http")

        os.environ["https_proxy"] = proxies.get("https")
        os.environ["HTTPS_PROXY"] = proxies.get("https")

    # SubscriberClient() must be used in a `with` block or have __enter__() called before use.
    with SubscriberClient(credentials=credentials, executor=executor) as subscriber_client:
        streaming_pull_future = subscriber_client.subscribe(
            subscription,
            callback=callback,
            per_partition_flow_control_settings=per_partition_flow_control_settings,
        )
        if timeout:
            try:
                streaming_pull_future.result(timeout=timeout)
            except Unauthenticated:
                print("Unauthenticated.....")
            except TimeoutError:
                streaming_pull_future.cancel()
                print("Successfully connected")
                logger.info("Connection established Successfully")
                assert streaming_pull_future.done()
            except KeyboardInterrupt:
                print("Got interrupted by keyboard")
            except Exception as e:
                print("Error occured: {}".format(e))
        else:
            logger.info(
                "Received params Outstanding messages: {} Messages.".format(messages_outstanding)
            )
            logger.info("Received params Outstanding bytes: {} Bytes.".format(bytes_outstanding))
            logger.info("Listening for messages on {}...".format(str(subscription)))
            print("Listening for messages on {}...".format(str(subscription)))
            streaming_pull_future.result()


if __name__ == "__main__":
    params = {
        Const.NSKP_TENANT_HOSTNAME: input("Tenant Hostname: "),
        Const.NSKP_TOKEN: getpass("Token V2: "),
        Const.NSKP_USER_AGENT: "test",
    }
    proxies = None
    proxy_enabled = input("Use Proxy (Y/N): ")

    if proxy_enabled == "Y" or proxy_enabled == "y":
        proxy_settings = {
            "proxy_url": input("Proxy Hostname: "),
            "proxy_port": input("Proxy port: "),
            "proxy_username": input("Proxy Username: "),
            "proxy_password": getpass("Proxy Password: "),
            "proxy_type": "http",
        }

        proxies = create_requests_proxies_helper(proxy_settings)
        params[Const.NSKP_PROXIES] = proxies
    sub_path_response = None
    sub_key_response = None
    try:
        # Create token_management client
        token_management = NetskopeTokenManagement(params)
        token_management_response = token_management.get()

        if token_management_response:
            if "subscription" in token_management_response:
                sub_path_response = token_management_response["subscription"]
            if "subscription-key" in token_management_response:
                sub_key_response = token_management_response["subscription-key"]
                print("Got subscription Key and Path.")
    except Exception as e:
        raise RequestException(e)

    try:
        stream(sub_key_response, sub_path_response, proxies)
    except Exception as e:
        import traceback
        print("Error occured: {}".format(traceback.format_exc())
