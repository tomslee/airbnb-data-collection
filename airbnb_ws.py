#!/usr/bin/python3
"""
Functions to request data from the Airbnb web site, and to manage
a set of requests.

Tom Slee, 2013--2017.
"""
import logging
import sys
import random
import time
import requests
from airbnb_config import ABConfig

# Set up logging
logger = logging.getLogger()


def ws_request_with_repeats(config, url, params=None):
    """ An attempt to get data from Airbnb. The function wraps
    a number of individual attempts, each of which may fail
    occasionally, in an attempt to get a more reliable
    data set.

    Returns None on failure
    """
    logger.debug("URL for this search: %s", url)
    for attempt_id in range(config.MAX_CONNECTION_ATTEMPTS):
        try:
            response = ws_individual_request(config, url, attempt_id, params)
            if response is None:
                continue
            elif response.status_code == requests.codes.ok:
                return response
        except (SystemExit, KeyboardInterrupt):
            raise
        except AttributeError:
            logger.exception("AttributeError retrieving page")
        except Exception as ex:
            logger.error("Failed to retrieve web page %s", url)
            logger.exception("Exception retrieving page: %s", str(type(ex)))
            # Failed
    return None


def ws_individual_request(config, url, attempt_id, params=None):
    """
    Individual web request: returns a response object or None on failure
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
        logger.debug("Using " + str(len(config.HTTP_PROXY_LIST)) + " proxies")
        if len(config.HTTP_PROXY_LIST) > 0:
            http_proxy = random.choice(config.HTTP_PROXY_LIST)
            proxies = {
                'http': http_proxy,
                'https': http_proxy,
            }
            logger.debug("Requesting page through proxy %s", http_proxy)
        else:
            proxies = None
            logger.debug("Requesting page without using a proxy")

        # Now make the request
        # cookie to avoid auto-redirect
        cookies = dict(sticky_locale='en')
        response = requests.get(url, params, timeout=timeout,
                                headers=headers, cookies=cookies, proxies=proxies)
        if response.status_code < 300:
            return response
        else:
            if http_proxy:
                logger.warning(
                    "HTTP status %s from web site: IP address %s may be blocked",
                    response.status_code, http_proxy)
                if len(config.HTTP_PROXY_LIST) > 0:
                    # randomly remove the proxy from the list, with probability 50%
                    if random.choice([True, False]):
                        config.HTTP_PROXY_LIST.remove(http_proxy)
                        logger.warning(
                            "Removing %s from proxy list; %s of %s remain",
                            http_proxy, len(config.HTTP_PROXY_LIST),
                            len(config.HTTP_PROXY_LIST_COMPLETE))
                    else:
                        logger.warning(
                            "Not removing %s from proxy list this time; still %s of %s",
                            http_proxy, len(config.HTTP_PROXY_LIST),
                            len(config.HTTP_PROXY_LIST_COMPLETE))
                if len(config.HTTP_PROXY_LIST) == 0:
                    # fill proxy list again, wait a long time, then restart
                    logger.warning(("No proxies remain."
                                    "Resetting proxy list and waiting %s minutes."),
                                   (config.RE_INIT_SLEEP_TIME / 60.0))
                    config.HTTP_PROXY_LIST = list(config.HTTP_PROXY_LIST_COMPLETE)
                    time.sleep(config.RE_INIT_SLEEP_TIME)
                    config.REQUEST_SLEEP += 1.0
                    logger.warning("Adding one second to request sleep time.  Now %s",
                                   config.REQUEST_SLEEP)
            else:
                logger.warning(("HTTP status %s from web site: IP address blocked."
                                "Waiting %s minutes."),
                               response.status_code, (config.RE_INIT_SLEEP_TIME / 60.0))
                time.sleep(config.RE_INIT_SLEEP_TIME)
                config.REQUEST_SLEEP += 1.0
            return response
    except (SystemExit, KeyboardInterrupt):
        raise
    except requests.exceptions.ConnectionError:
        # For requests error and exceptions, see
        # http://docs.python-requests.org/en/latest/user/quickstart/
        # errors-and-exceptions
        logger.warning("Network request %s: connectionError. Bad proxy %s?",
                       attempt_id, http_proxy)
        return None
    except requests.exceptions.HTTPError:
        logger.error("Network request exception %s: invalid HTTP response", attempt_id)
        return None
    except requests.exceptions.Timeout:
        logger.warning("Network request exception %s: timeout", attempt_id)
        return None
    except requests.exceptions.TooManyRedirects:
        logger.error("Network request exception %s: too many redirects", attempt_id)
        return None
    except requests.exceptions.RequestException:
        logger.error("Network request exception %s: unidentified requests", attempt_id)
        return None
    except Exception as e:
        logger.exception("Network request exception: type %s", type(e).__name__)
        return None
