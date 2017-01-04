#!/usr/bin/python3
# ============================================================================
# Airbnb Configuration module, for use in web scraping and analytics
# ============================================================================
import logging
import psycopg2
import psycopg2.errorcodes
import os
import configparser

class ABConfig():

    def __init__(self):
        """ Read the configuration file <username>.config to set up the run
        """
        self.connection = None
        try:
            config = configparser.ConfigParser()

            # look for username.config on both Windows (USERNAME) and Linux (USER)
            if os.name == "nt":
                username = os.environ['USERNAME']
            else:
                username = os.environ['USER']
            config_file = username + ".config"
            if not os.path.isfile(config_file):
                logging.error("Configuration file " + config_file + " not found.")
                sys.exit()
            config.read(config_file)

            # database
            try:
                self.DB_HOST = config["DATABASE"]["db_host"] if ("db_host" in config["DATABASE"]) else None
                self.DB_PORT = config["DATABASE"]["db_port"]
                self.DB_NAME = config["DATABASE"]["db_name"]
                self.DB_USER = config["DATABASE"]["db_user"]
                self.DB_PASSWORD = config["DATABASE"]["db_password"]
            except Exception:
                logging.error("Incomplete database information in " + config_file + ": cannot continue.")
                sys.exit()
            # network
            try:
                self.HTTP_PROXY_LIST = config["NETWORK"]["proxy_list"].split(",")
                self.HTTP_PROXY_LIST = [x.strip() for x in self.HTTP_PROXY_LIST]
            except Exception as ex:
                logging.warning("No proxy_list in " + config_file + ": not using proxies")
                self.HTTP_PROXY_LIST = []
            try:
                self.USER_AGENT_LIST = config["NETWORK"]["user_agent_list"].split(",,")
                self.USER_AGENT_LIST = [x.strip() for x in self.USER_AGENT_LIST]
                self.USER_AGENT_LIST = [x.strip('"') for x in self.USER_AGENT_LIST]
            except Exception:
                logging.info("No user agent list in " + username +
                            ".config: not using user agents")
                self.USER_AGENT_LIST = []
            self.MAX_CONNECTION_ATTEMPTS = \
                int(config["NETWORK"]["max_connection_attempts"])
            self.REQUEST_SLEEP = float(config["NETWORK"]["request_sleep"])
            self.HTTP_TIMEOUT = float(config["NETWORK"]["http_timeout"])

            # survey
            self.FILL_MAX_ROOM_COUNT = int(config["SURVEY"]["fill_max_room_count"])
            self.ROOM_ID_UPPER_BOUND = int(config["SURVEY"]["room_id_upper_bound"])
            self.SEARCH_MAX_PAGES = int(config["SURVEY"]["search_max_pages"])
            self.SEARCH_MAX_GUESTS = int(config["SURVEY"]["search_max_guests"])
            self.SEARCH_MAX_RECTANGLE_ZOOM = int(
                config["SURVEY"]["search_max_rectangle_zoom"])
            self.RE_INIT_SLEEP_TIME = float(config["SURVEY"]["re_init_sleep_time"])

        except Exception:
            logging.exception("Failed to read config file properly")
            raise

    # get a database connection
    def connect(self):
        """ Return a connection to the database"""
        try:
            if (not hasattr(self, "connection") or
                self.connection is None or self.connection.closed != 0):
                cattr = dict(
                    user=self.DB_USER,
                    password=self.DB_PASSWORD,
                    database=self.DB_NAME
                )
                if self.DB_HOST is not None:
                    cattr.update(dict(
                                host=self.DB_HOST,
                                port=self.DB_PORT,
                                ))
                self.connection = psycopg2.connect(**cattr)
                self.connection.set_client_encoding('UTF8')
            return self.connection
        except psycopg2.OperationalError as pgoe:
            logging.error(pgoe.message)
            raise
        except Exception:
            logging.error("Failed to connect to database.")
            raise
