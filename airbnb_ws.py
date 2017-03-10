#!/usr/bin/python3
# ============================================================================
# Tom Slee, 2013--2017.
#
# An ABListing represents and individual Airbnb listing
# ============================================================================
import logging
import sys
import random
import time
import requests
from airbnb_config import ABConfig

# Set up logging
logger = logging.getLogger(__name__)


def ws_request_with_repeats(config, url, params=None):
    # Return None on failure
    for attempt in range(config.MAX_CONNECTION_ATTEMPTS):
        try:
            response = ws_request(config, url, params)
            if response is None:
                logger.warning("Request failure " + str(attempt + 1) +
                               ": trying again")
            elif response.status_code == requests.codes.ok:
                return response
        except AttributeError:
            logger.exception("AttributeError retrieving page")
        except Exception as ex:
            logger.error("Failed to retrieve web page " + url)
            logger.exception("Exception retrieving page: " + str(type(ex)))
            # logger.error("Exception type: " + type(e).__name__)
            # Failed
    return None


def ws_request(config, url, params=None):
    """
    Individual web request: returns a response object
    """
    try:
        # wait
        sleep_time = config.REQUEST_SLEEP * random.random()
        logger.debug("sleeping " + str(sleep_time)[:7] + " seconds...")
        time.sleep(sleep_time)  # be nice

        timeout = config.HTTP_TIMEOUT

        # If a list of user agent strings is supplied, use it
        if len(config.USER_AGENT_LIST) > 0:
            user_agent = random.choice(config.USER_AGENT_LIST)
            headers = {"User-Agent": user_agent}
        else:
            headers = {'User-Agent': 'Mozilla/5.0'}

        # If there is a list of proxies supplied, use it
        http_proxy = None
        logger.debug("Using " + str(len(config.HTTP_PROXY_LIST)) + " proxies.")
        if len(config.HTTP_PROXY_LIST) > 0:
            http_proxy = random.choice(config.HTTP_PROXY_LIST)
            proxies = {
                'http': http_proxy,
                'https': http_proxy,
            }
            logger.debug("Requesting page through proxy " + http_proxy)
        else:
            proxies = None

        # Now make the request
        response = requests.get(url, params, timeout=timeout,
                                headers=headers, proxies=proxies)
        if response.status_code == 503:
            if http_proxy:
                logger.warning("503 error for proxy " + http_proxy)
            else:
                logger.warning("503 error (no proxy)")
            if random.choice([True, False]):
                logger.info("Removing " + http_proxy + " from proxy list.")
                config.HTTP_PROXY_LIST.remove(http_proxy)
                if len(config.HTTP_PROXY_LIST) < 1:
                    # fill proxy list again, wait a long time, then restart
                    logger.error("No proxies in list. Re-initializing.")
                    time.sleep(config.RE_INIT_SLEEP_TIME)  # be nice
                    config = ABConfig()
        return response
    except KeyboardInterrupt:
        logger.error("Cancelled by user")
        sys.exit()
    except requests.exceptions.ConnectionError:
        # For requests error and exceptions, see
        # http://docs.python-requests.org/en/latest/user/quickstart/
        # errors-and-exceptions
        logger.error("Network problem: ConnectionError")
        if random.choice([True, False]):
            if http_proxy is None or len(config.HTTP_PROXY_LIST) < 1:
                # fill the proxy list again, and wait a long time, then restart
                logger.error("No proxies left in the list. Re-initializing.")
                time.sleep(config.RE_INIT_SLEEP_TIME)  # be nice

            else:
                # remove the proxy from the proxy list
                logger.warning("Removing " + http_proxy + " from proxy list.")
                config.HTTP_PROXY_LIST.remove(http_proxy)
        return None
    except requests.exceptions.HTTPError:
        logger.error("Invalid HTTP response: HTTPError")
        return None
    except requests.exceptions.Timeout:
        logger.error("Request timed out: Timeout")
        return None
    except requests.exceptions.TooManyRedirects:
        logger.error("Too many redirects: TooManyRedirects")
        return None
    except requests.exceptions.RequestException:
        logger.error("Unidentified Requests error: RequestException")
        return None
    except Exception as e:
        logger.exception("Exception type: " + type(e).__name__)
        return None
