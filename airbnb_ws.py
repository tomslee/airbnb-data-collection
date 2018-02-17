#!/usr/bin/python3
# ============================================================================
# Tom Slee, 2013--2017.
#
# Functions for making and handling requests from the Airbnb web site
# TODO: Make this into a class
# ============================================================================
import logging
import sys
import random
import time
import requests
from airbnb_config import ABConfig

# Set up logging
logger = logging.getLogger()


def ws_request_with_repeats(config, url, params=None):
    # Return None on failure
    logger.debug(url)
    for attempt_id in range(config.MAX_CONNECTION_ATTEMPTS):
        try:
            response = ws_request(config, url, attempt_id, params)
            if response is None:
                continue
            elif response.status_code == requests.codes.ok:
                return response
        except (SystemExit, KeyboardInterrupt):
            raise
        except AttributeError:
            logger.exception("AttributeError retrieving page")
        except Exception as ex:
            logger.error("Failed to retrieve web page " + url)
            logger.exception("Exception retrieving page: " + str(type(ex)))
            # Failed
    return None


def ws_request(config, url, attempt_id, params=None):
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
        # cookie to avoid auto-redirect
        cookies = dict(sticky_locale='en')
        response = requests.get(url, params, timeout=timeout,
                headers=headers, cookies=cookies, proxies=proxies)
        if response.status_code < 300:
            return response
        else:
            if http_proxy:
                logger.warning("HTTP status {s} from web site: IP address {a} may be blocked"
                    .format(s=response.status_code, a=http_proxy))
                if len(config.HTTP_PROXY_LIST) > 0:
                    # randomly remove the proxy from the list, with probability 50%
                    if random.choice([True, False]):
                        config.HTTP_PROXY_LIST.remove(http_proxy)
                        logger.warning(
                            "Removing {http_proxy} from proxy list; {n} of {p} remain."
                            .format( http_proxy=http_proxy,
                                n=len(config.HTTP_PROXY_LIST),
                                p=len(config.HTTP_PROXY_LIST_COMPLETE)))
                    else:
                        logger.warning(
                            "Not removing {http_proxy} from proxy list this time; still {n} of {p}."
                            .format( http_proxy=http_proxy,
                                n=len(config.HTTP_PROXY_LIST),
                                p=len(config.HTTP_PROXY_LIST_COMPLETE)))
                if len(config.HTTP_PROXY_LIST) == 0:
                    # fill proxy list again, wait a long time, then restart
                    logger.warning("No proxies remain. Resetting proxy list and waiting {m} minutes."
                        .format(m=(config.RE_INIT_SLEEP_TIME / 60.0)))
                    config.HTTP_PROXY_LIST = list(config.HTTP_PROXY_LIST_COMPLETE)
                    time.sleep(config.RE_INIT_SLEEP_TIME)
                    config.REQUEST_SLEEP += 1.0
                    logger.warning("Adding one second to request sleep time. Now {s}"
                        .format(s=config.REQUEST_SLEEP))
            else:
                logger.warning("HTTP status {s} from web site: IP address blocked. Waiting {m} minutes."
                        .format(s=response.status_code, m=(config.RE_INIT_SLEEP_TIME / 60.0)))
                time.sleep(config.RE_INIT_SLEEP_TIME)
                config.REQUEST_SLEEP += 1.0
            return response
    except (SystemExit, KeyboardInterrupt):
        raise
    except requests.exceptions.ConnectionError:
        # For requests error and exceptions, see
        # http://docs.python-requests.org/en/latest/user/quickstart/
        # errors-and-exceptions
        logger.warning("Network request exception {a}: connectionError".format(a=attempt_id))
        return None
    except requests.exceptions.HTTPError:
        logger.error("Network request exception {a}: invalid HTTP response".format(a=attempt_id))
        return None
    except requests.exceptions.Timeout:
        logger.warning("Network request exception {a}: timeout".format(a=attempt_id))
        return None
    except requests.exceptions.TooManyRedirects:
        logger.error("Network request exception {a}: too many redirects".format(a=attempt_id))
        return None
    except requests.exceptions.RequestException:
        logger.error("Network request exception {a}: unidentified requests".format(a=attempt_id))
        return None
    except Exception as e:
        logger.exception("Network request exception: type " + type(e).__name__)
        return None
