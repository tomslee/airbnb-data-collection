#!/usr/bin/python3
# ============================================================================
# Airbnb web site scraper, for analysis of Airbnb listings
# Tom Slee, 2013--2015.
#
# function naming conventions:
#   ws_get = get from web site
#   db_get = get from database
#   db_add = add to the database
#
# function name conventions:
#   add = add to database
#   display = open a browser and show
#   list = get from database and print
#   print = get from web site and print
# ============================================================================
import re
import logging
import argparse
import sys
import time
import random
import subprocess
import requests
import urllib.request
import urllib.parse
import urllib.error
from lxml import html
import psycopg2
import psycopg2.errorcodes
import webbrowser
import os
import configparser
import json

# ============================================================================
# CONSTANTS
# ============================================================================
# Database: filled in from <username>.config
DB_HOST = None
DB_PORT = None
DB_NAME = None
DB_USER = None
DB_PASSWORD = None

# Network management: filled in from <username>.config
HTTP_TIMEOUT = None
HTTP_PROXY_LIST = []
MAX_CONNECTION_ATTEMPTS = None
REQUEST_SLEEP = None

# Survey characteristics: filled in from <username>.config
FILL_MAX_ROOM_COUNT = None
ROOM_ID_UPPER_BOUND = None  # max(room_id) = 5,548,539 at start
SEARCH_MAX_PAGES = None
SEARCH_MAX_GUESTS = None

# URLs (fixed)
URL_ROOT = "http://www.airbnb.com/"
URL_ROOM_ROOT =URL_ROOT + "rooms/"
URL_HOST_ROOT = URL_ROOT + "users/show/"
URL_SEARCH_ROOT = URL_ROOT + "s/"

# Other internal constants
SEARCH_AREA_GLOBAL="UNKNOWN" # special case: sample listings globally
FLAGS_ADD = 1
FLAGS_PRINT = 9
FLAGS_INSERT_REPLACE = True
FLAGS_INSERT_NO_REPLACE = False
RE_INIT_SLEEP_TIME = 1800 # seconds
SEARCH_BY_NEIGHBORHOOD=0 # default
SEARCH_BY_ZIPCODE=1

# Script version
# 2.3 released Jan 12, 2015, to handle a web site update
SCRIPT_VERSION_NUMBER = 2.4

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

ch_formatter = logging.Formatter('%(levelname)-8s%(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(ch_formatter)
logger.addHandler(console_handler)

fl_formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s%(message)s')
filelog_handler = logging.FileHandler("run.log", encoding="utf-8")
filelog_handler.setLevel(logging.DEBUG)
filelog_handler.setFormatter(fl_formatter)
logger.addHandler(filelog_handler)

# global database connection
_conn = None

def init():
    try:
        config = configparser.ConfigParser()
        # look for username.config on both Windows (USERNAME) and Linux (USER)
        if os.name == "nt":
            username = os.environ['USERNAME']
        else:
            username = os.environ['USER']
        config_file = username + ".config"
        if not os.path.isfile(config_file):
            logging.error("Configuration file " + config_file +
            " not found.")
            sys.exit()
        config.read(config_file)
        # database
        global DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
        DB_HOST = config["DATABASE"]["db_host"]
        DB_PORT = config["DATABASE"]["db_port"]
        DB_NAME = config["DATABASE"]["db_name"]
        DB_USER = config["DATABASE"]["db_user"]
        DB_PASSWORD = config["DATABASE"]["db_password"]
        # network
        global HTTP_PROXY_LIST, MAX_CONNECTION_ATTEMPTS, REQUEST_SLEEP, HTTP_TIMEOUT
        try:
            HTTP_PROXY_LIST = config["NETWORK"]["proxy_list"].split(",")
        except Exception:
            logging.info("No http_proxy_list in " + username + ".config: not using proxies")
            HTTP_PROXY_LIST = None
        MAX_CONNECTION_ATTEMPTS = int(config["NETWORK"]["max_connection_attempts"])
        REQUEST_SLEEP = float(config["NETWORK"]["request_sleep"])
        HTTP_TIMEOUT = float(config["NETWORK"]["http_timeout"])
        # survey
        global FILL_MAX_ROOM_COUNT, ROOM_ID_UPPER_BOUND, SEARCH_MAX_PAGES, SEARCH_MAX_GUESTS
        FILL_MAX_ROOM_COUNT = int(config["SURVEY"]["fill_max_room_count"])
        ROOM_ID_UPPER_BOUND = int(config["SURVEY"]["room_id_upper_bound"])
        SEARCH_MAX_PAGES = int(config["SURVEY"]["search_max_pages"])
        SEARCH_MAX_GUESTS = int(config["SURVEY"]["search_max_guests"])
    except Exception:
        logger.exception("Failed to read config file properly")
        raise

# get a database connection
def connect():
    try:
        global _conn
        if _conn is None:
            _conn = psycopg2.connect(
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME)
            _conn.set_client_encoding('UTF8')
        return _conn
    except psycopg2.OperationalError as pgoe:
        logger.error(pgoe.message)
        raise
    except Exception:
        logger.error("Failed to connect to database.")
        raise


# =============================================================================
# Listing represents an Airbnb room_id, as captured at a moment in time. 
# room_id, survey_id is the primary key.
# Occasionally, a survey_id = None will happen, but for retrieving data 
# straight from the web site, and not stored in the database.
# =============================================================================
class Listing():
    def __init__(self, room_id, survey_id, room_type = None):
        self.room_id = room_id
        self.host_id = None
        self.room_type = room_type
        self.country = None
        self.city = None
        self.neighborhood = None
        self.address = None
        self.reviews = None
        self.overall_satisfaction = None   
        self.accommodates = None
        self.bedrooms = None
        self.bathrooms = None
        self.price = None   
        self.deleted = 0
        self.minstay = None
        self.latitude = None
        self.longitude = None
        self.survey_id  = survey_id
        self.last_modified = None

    def has_been_deleted(self):
        deleted = False
        assigned_values = [attr for attr in dir(self) 
                if not callable(attr) 
                and not attr.startswith("__")
                and not None]
        if len(assigned_values) < 6: #just a value
            deleted = True
        return deleted

    def get_columns(self):
        """
        Hack: callable(attr) includes methods with (self) as argument. Need to find a way to avoid these.
        This hack does also provide the proper order, which matters
        """
        # columns = [attr for attr in dir(self) if not callable(attr) and not attr.startswith("__")]
        columns = ("room_id", "host_id", "room_type", "country",
            "city", "neighborhood", "address", "reviews", "overall_satisfaction",
            "accommodates", "bedrooms", "bathrooms", "price", "deleted", "minstay",
            "latitude", "longitude", "survey_id", "last_modified",)
        return columns


    def save_as_deleted(self):
        try:
            if self.survey_id == None: return
            conn = connect()
            sql = """
                update room 
                set deleted = 1, last_modified = now()::timestamp
                where room_id = %s 
                and survey_id = %s
            """
            cur = conn.cursor()
            cur.execute(sql, (self.room_id, self.survey_id))
            cur.close()
            conn.commit()
        except Exception:
            logger.error("Failed to save room as deleted")
            raise

    def save(self, insert_replace_flag):
        try:
            # does the room already exist?
            conn = connect()
            cur = conn.cursor()
            sql_select = """select count(*) 
                from room 
                where room_id = %s 
                and survey_id = %s"""
            cur.execute(sql_select, (self.room_id, self.survey_id,))
            room_exists = bool(cur.fetchone()[0])
            if room_exists:
                if self.deleted == 1:
                    self.save_as_deleted()
                elif insert_replace_flag==FLAGS_INSERT_REPLACE:
                    self.__update()
            else:
                self.__insert()
        except psycopg2.DatabaseError as pgdbe:
            # connection closed
            logger.error("Database error: set conn to None and resume")
            conn = None
        except psycopg2.InterfaceError as pgie:
            # connection closed
            logger.error("Interface error: set conn to None and resume")
            conn = None
        except psycopg2.Error as pge:
            # database error: rollback operations and resume
            cur.close()
            conn.rollback()
            if insert_replace_flag:
                logger.error("Database error: " + str(self.room_id))
                logger.error("Diagnostics " + pge.diag.message_primary)
            else:
                logger.info("Listing already saved: " + str(self.room_id))
                pass   # not a problem
        except ValueError as ve:
            logger.error("ValueError for room_id = " + str(self.room_id))
            cur.close()
        except KeyboardInterrupt:
            cur.close()
            conn.rollback()
            raise
        except UnicodeEncodeError as uee:
            logger.error("UnicodeEncodeError Exception at " + 
                    str(uee.object[uee.start:uee.end])) 
            raise
        except AttributeError as ae:
            logger.error("AttributeError")
            raise
        except Exception as e:
            cur.close()
            conn.rollback()
            logger.error("Exception saving room")
            raise
    def print_from_web_site(self):
        try:
            s = "Room info:"
            s += "\n\troom_id:\t" + str(self.room_id)
            s += "\n\tsurvey_id:\t" + str(self.survey_id)
            s += "\n\thost_id:\t" + str(self.host_id)
            s += "\n\troom_type:\t" + str(self.room_type)
            s += "\n\tcountry:\t" + str(self.country)
                #print("\tcity:", str(city.encode(encoding="cp850", errors="ignore")))
            s += "\n\tcity:\t\t" + str(self.city)
            s += "\n\tneighborhood:\t" + str(self.neighborhood)
            s += "\n\taddress:\t" + str(self.address)
            s += "\n\treviews:\t" + str(self.reviews)
            s += "\n\toverall_satisfaction:\t" + str(self.overall_satisfaction)
            s += "\n\taccommodates:\t" + str(self.accommodates)
            s += "\n\tbedrooms:\t" + str(self.bedrooms)
            s += "\n\tbathrooms:\t" + str(self.bathrooms)
            s += "\n\tprice:\t\t" + str(self.price)
            s += "\n\tdeleted:\t" + str(self.deleted)
            s += "\n\tlatitude:\t" + str(self.latitude)
            s += "\n\tlongitude:\t" + str(self.longitude)
            s += "\n\tminstay:\t" + str(self.minstay)
            print(s)
        except Exception:
            raise

    def print_from_db(self):
        try:
            columns = self.get_columns()
            sql = "select room_id"
            for column in columns[1:]:
                sql += ", " + column
            sql += " from room where room_id = %s"
            conn = connect()
            cur = conn.cursor()
            cur.execute(sql, (self.room_id,))
            result_set = cur.fetchall()
            if len(result_set) > 0:
                for result in result_set:
                    i = 0
                    print("Room information: ")
                    for column in columns:
                        print("\t", column, "=", str(result[i]))
                        i += 1
                return True
            else:
                print("\nThere is no room", str(self.room_id), "in the database.\n")
                return False
            cur.close()
        except Exception:
            raise

    def ws_get_room_info(self, flag):
        try:
            # initialization
            logger.info("-" * 70)
            logger.info("Getting room " + str(listing.room_id)
                        + " from Airbnb web site")
            room_url = URL_ROOM_ROOT + str(listing.room_id)
            page = ws_get_page(room_url)
            if page is not False:
                self.__get_room_info_from_page(page, flag)
                return True
            else:
                return False
        except BrokenPipeError as bpe:
            logger.error(bpe.message)
            raise
        except KeyboardInterrupt:
            logger.error("Keyboard interrupt")
            raise
        except UnicodeEncodeError as uee:
            logger.error("UnicodeEncodeError Exception at " + 
                    str(uee.object[uee.start:uee.end])) 
        except urllib.http.HTTPError:
            # mainly 503 errors: handle them above here
            raise
        except Exception as e:
            logger.error("Failed to get room " + str(listing.room_id) + " from web site.")
            logger.error("Exception: " + str(type(e)))
            raise

    def __insert(self):
        try:
            conn = connect()
            cur = conn.cursor()
            logger.debug("Inserting... ")
            sql = """
                insert into room (
                    room_id, host_id, room_type, country, city,
                    neighborhood, address, reviews, overall_satisfaction, accommodates,
                    bedrooms, bathrooms, price, deleted, minstay,
                    latitude, longitude, survey_id
                )
                """
            sql += """
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
                )"""
            insert_args = (
                self.room_id, self.host_id, self.room_type, self.country, self.city,
                self.neighborhood, self.address, self.reviews, self.overall_satisfaction,
                self.accommodates, self.bedrooms, self.bathrooms, self.price,
                self.deleted, self.minstay, self.latitude, self.longitude, self.survey_id,
                )
            cur.execute(sql, insert_args)
            conn.commit()
            cur.close()
            logger.info("Inserted room " + str(self.room_id))
        except:
            raise

    def __update(self):
        try:
            conn = connect()
            cur = conn.cursor()
            logger.debug("Updating...")
            sql = """
                update room 
                set host_id = %s, room_type = %s,
                    country = %s, city = %s, neighborhood = %s,
                    address = %s, reviews = %s, overall_satisfaction = %s,
                    accommodates = %s, bedrooms = %s, bathrooms = %s,
                    price = %s, deleted = %s, last_modified = now()::timestamp,
                    minstay = %s, latitude = %s, longitude = %s
                where room_id = %s 
                and survey_id = %s"""
            update_args = (
                self.host_id,
                self.room_type, self.country, self.city, self.neighborhood, self.address,
                self.reviews, self.overall_satisfaction, self.accommodates, self.bedrooms,
                self.bathrooms, self.price, self.deleted, self.minstay, self.latitude,
                self.longitude, self.room_id, self.survey_id,
                )
            cur.execute(sql, update_args)
            conn.commit()
            cur.close()
            logger.info("Inserted room " + str(self.room_id))
        except:
            raise

    def __get_room_info_from_page(page, listing, flag):
        try:
            tree = html.fromstring(page)
            if tree is None:
                listing.deleted = 1
            else:
                listing.deleted = 0

            # Some of these items do not appear on every page (eg,
            # ratings, bathrooms), and so their absence is marked with
            # logger.info. Others should be present for every room (eg,
            # latitude, room_type, host_id) and so are marked with a
            # warning.  Items coded in <meta
            # property="airbedandbreakfast:*> elements -- country --

            # -- country --
            temp = tree.xpath(
                "//meta[contains(@property,'airbedandbreakfast:country')]"
                "/@content"
                )
            if len(temp) > 0:
                listing.country = temp[0]
            else:
                logger.info("No country found for room " + str(listing.room_id))

            # -- city --
            temp = tree.xpath(
                "//meta[contains(@property,'airbedandbreakfast:city')]"
                "/@content"
                )
            if len(temp) > 0:
                #city = UnicodeDammit(temp[0]).unicode_markup
                listing.city = temp[0]
            else:
                logger.warning("No city found for room " + str(listing.room_id))

            # -- rating --
            temp = tree.xpath(
                "//meta[contains(@property,'airbedandbreakfast:rating')]"
                "/@content"
                )
            if len(temp) > 0:
                listing.overall_satisfaction = temp[0]
            else:
                logger.info("No rating found for room " + str(listing.room_id))

            # -- latitude --
            temp = tree.xpath("//meta"
                              "[contains(@property,"
                              "'airbedandbreakfast:location:latitude')]"
                              "/@content")
            if len(temp) > 0:
                listing.latitude = temp[0]
            else:
                logger.warning("No latitude found for room " + str(listing.room_id))

            # -- longitude --
            temp = tree.xpath(
                "//meta"
                "[contains(@property,'airbedandbreakfast:location:longitude')]"
                "/@content")
            if len(temp) > 0:
                listing.longitude = temp[0]
            else:
                logger.warning("No longitude found for room " + str(listing.room_id))

            # -- host_id --
            temp = tree.xpath(
                "//div[@id='host-profile']"
                "//a[contains(@href,'/users/show')]"
                "/@href"
            )
            if len(temp) > 0:
                host_id_element = temp[0]
                host_id_offset = len('/users/show/')
                listing.host_id = int(host_id_element[host_id_offset:])
            else:
                temp = tree.xpath(
                    "//div[@id='user']"
                    "//a[contains(@href,'/users/show')]"
                    "/@href")
                if len(temp) > 0:
                    host_id_element = temp[0]
                    host_id_offset = len('/users/show/')
                    listing.host_id = int(host_id_element[host_id_offset:])
                else:
                    logger.warning("No host_id found for room " + str(listing.room_id))

            # -- room type --
            # new page format 2015-09-30?
            listing.room_type = "Unknown"
            temp = tree.xpath(
                "//div[@class='col-md-6']"
                "/div/span[text()[contains(.,'Room type:')]]"
                "/../strong/text()"
                )
            if len(temp) > 0:
                listing.room_type = temp[0].strip()
            else:
                # new page format 2014-12-26
                temp_entire = tree.xpath(
                    "//div[@id='summary']"
                    "//i[contains(concat(' ', @class, ' '), ' icon-entire-place ')]"
                    )
                if len(temp_entire) > 0:
                    room_type = "Entire home/apt"
                temp_private = tree.xpath(
                    "//div[@id='summary']"
                    "//i[contains(concat(' ', @class, ' '), ' icon-private-room ')]"
                    )
                if len(temp_private) > 0:
                    room_type = "Private room"
                temp_shared = tree.xpath(
                    "//div[@id='summary']"
                    "//i[contains(concat(' ', @class, ' '), ' icon-shared-room ')]"
                    )
                if len(temp_shared) > 0:
                    listing.room_type = "Shared room"
            if listing.room_type == 'Unknown':
                logger.warning("No room_type found for room " + str(listing.room_id))

            # -- neighborhood --
            temp2 = tree.xpath(
                "//div[contains(@class,'rich-toggle')]/@data-address"
                )
            temp1 = tree.xpath("//table[@id='description_details']"
                "//td[text()[contains(.,'Neighborhood:')]]"
                "/following-sibling::td/descendant::text()")
            if len(temp2) > 0:
                s = temp2[0].strip()
                listing.neighborhood = s[s.find("(")+1:s.find(")")]
            elif len(temp1) > 0:
                listing.neighborhood = temp1[0].strip()
            else:
                logger.warning("No neighborhood found for room "
                                   + str(listing.room_id))
            if listing.neighborhood is not None:
                listing.neighborhood = listing.neighborhood[:50]

            # -- address --
            temp = tree.xpath(
                "//div[contains(@class,'rich-toggle')]/@data-address"
                )
            if len(temp) > 0:
                s = temp[0].strip()
                listing.address = s[:s.find(",")]
            else:
                # try old page match
                temp = tree.xpath(
                    "//span[@id='display-address']"
                    "/@data-location"
                    )
                if len(temp) > 0:
                    listing.address = temp[0]
                else:
                    logger.info("No address found for room " + str(listing.room_id))

            # -- reviews --
            # 2015-10-02
            temp2 = tree.xpath(
                    "//div[@class='___iso-state___p3summarybundlejs']"
                    "/@data-state"
                    )
            if len(temp2) == 1:
                summary = json.loads(temp2[0])
                listing.reviews = summary["visibleReviewCount"]
            elif len(temp2) == 0:
                temp = tree.xpath("//div[@id='room']/div[@id='reviews']//h4/text()")
                listing.reviews = temp[0].strip()
                listing.reviews = reviews.split('+')[0]
                listing.reviews = reviews.split(' ')[0].strip()
                if listing.reviews == "No":
                    listing.reviews = 0
            else:
                # try old page match
                temp = tree.xpath(
                    "//span[@itemprop='reviewCount']/text()"
                    )
                if len(temp) > 0:
                    listing.reviews = temp[0]
                else:
                    logger.info("No reviews found for room " + str(listing.room_id))

            # -- accommodates --
            # new version Dec 2014
            temp = tree.xpath(
                "//div[@class='col-md-6']"
                "/div/span[text()[contains(.,'Accommodates:')]]"
                "/../strong/text()"
                    )
            if len(temp) > 0:
                listing.accommodates = temp[0].strip()
                logging.debug("Accommodates " + str(listing.accommodates))
            else:
                temp = tree.xpath(
                    "//div[@class='col-md-6']"
                    "/div[text()[contains(.,'Accommodates:')]]"
                    "/strong/text()"
                    )
                if len(temp) > 0:
                    listing.accommodates = temp[0].strip()
                else:
                    temp = tree.xpath(
                    "//div[@class='col-md-6']"
                    "//div[text()[contains(.,'Accommodates:')]]"
                    "/strong/text()"
                    )
                    if len(temp) > 0:
                        listing.accommodates = temp[0].strip()
            if listing.accommodates:
                listing.accommodates = listing.accommodates.split('+')[0]
                listing.accommodates = listing.accommodates.split(' ')[0]
            else:
                logger.warning("No accommodates found for room "
                               + str(listing.room_id))

            # -- bedrooms --
            # new version Dec 2014
            temp = tree.xpath(
                "//div[@class='col-md-6']"
                "/div/span[text()[contains(.,'Bedrooms:')]]"
                "/../strong/text()"
                )
            if len(temp) > 0:
                listing.bedrooms = temp[0].strip()
            else:
                temp = tree.xpath(
                    "//div[@class='col-md-6']"
                    "/div[text()[contains(.,'Bedrooms:')]]"
                    "/strong/text()"
                    )
                if len(temp) > 0:
                    listing.bedrooms = temp[0].strip()
            if listing.bedrooms:
                listing.bedrooms = listing.bedrooms.split('+')[0]
                listing.bedrooms = listing.bedrooms.split(' ')[0]
            else:
                logger.warning("No bedrooms found for room " + str(listing.room_id))

            # -- bathrooms --
            temp3 = tree.xpath(
                "//div[@class='col-md-6']"
                "/div/span[text()[contains(.,'Bathrooms:')]]"
                "/../strong/text()"
                )
            if len(temp) > 0:
                listing.bathrooms = temp[0].strip()
            else:
                temp = tree.xpath(
                    "//div[@class='col-md-6']"
                    "/div/span[text()[contains(.,'Bathrooms:')]]"
                    "/../strong/text()"
                    )
                if len(temp) > 0:
                    listing.bathrooms = temp[0].strip()
            if listing.bathrooms:
                listing.bathrooms = listing.bathrooms.split('+')[0]
                listing.bathrooms = listing.bathrooms.split(' ')[0]
            else:
                logger.info("No bathrooms found for room " + str(listing.room_id))

            # -- minimum stay --
            temp3 = tree.xpath(
                "//div[contains(@class,'col-md-6')"
                "and text()[contains(.,'minimum stay')]]"
                "/strong/text()"
                )
            temp2 = tree.xpath(
                "//div[@id='details-column']"
                "//div[contains(text(),'Minimum Stay:')]"
                "/strong/text()"
                )
            temp1 = tree.xpath(
                "//table[@id='description_details']"
                "//td[text()[contains(.,'Minimum Stay:')]]"
                "/following-sibling::td/descendant::text()"
                )
            if len(temp3) > 0:
                listing.minstay = temp3[0].strip()
            elif len(temp2) > 0:
                listing.minstay = temp2[0].strip()
            elif len(temp1) > 0:
                listing.minstay = temp1[0].strip()
            else:
                logger.info("No minstay found for room " + str(listing.room_id))
            if listing.minstay is not None:
                listing.minstay = listing.minstay.split('+')[0]
                listing.minstay = listing.minstay.split(' ')[0]

            # -- price --
            # Price is returned in the currency where your request comes from
            temp2 = tree.xpath(
                "//meta[@itemprop='price']/@content"
                )
            temp1 = tree.xpath(
                    "//div[@id='price_amount']/text()"
                )
            if len(temp2) > 0:
                listing.price=temp2[0]
            elif len(temp1) > 0:
                listing.price = temp[0][1:]
                non_decimal = re.compile(r'[^\d.]+')
                listing.price = non_decimal.sub('', price)
            else:
                # old page match is the same
                logger.info("No price found for room " + str(listing.room_id))
            # Now find out if it's per night or per month (see if the per_night div
            # is hidden)
            per_month = tree.xpath("//div[@class='js-per-night book-it__payment-period  hide']")
            if per_month:
                listing.price = int(int(listing.price) / 30)

            if listing.has_been_deleted():
                logger.warn("Room " + str(listing.room_id) + " has probably been deleted")
                listing.deleted = 1
            if flag == FLAGS_ADD:
                listing.print_from_web_site()
                listing.save(FLAGS_INSERT_REPLACE)
            elif flag == FLAGS_PRINT:
                listing.print_from_web_site()
            return True
        except KeyboardInterrupt:
            raise
        except IndexError:
            logger.error("Web page has unexpected structure.")
            raise
        except UnicodeEncodeError as uee:
            logger.error("UnicodeEncodeError Exception at " + 
                str(uee.object[uee.start:uee.end])) 
            raise
        except AttributeError as ae:
            logger.error("AttributeError: " + ae.message)
            raise
        except Exception as e:
            logger.exception("Error parsing web page.")
            raise

# ==============================================================================
# End of class Listing
# ==============================================================================

# ==============================================================================
# Survey class: information and methods around a search
# ==============================================================================
class Survey():
    def __init__(self, survey_id):
        self.survey_id = survey_id
        self.search_area_id = None
        self.search_area_name = None
        self.__set_search_area()

    def search(self, flag, search_by):
        try:
            if self.search_area_name==SEARCH_AREA_GLOBAL:
                # "Special case": global search
                room_count = 0
                while room_count < FILL_MAX_ROOM_COUNT:
                    try:
                        # get a randome candidate room_id
                        room_id = random.randint(0, ROOM_ID_UPPER_BOUND)
                        listing = Listing(room_id, self.survey_id)
                        if room_id is None:
                            break
                        else:
                            sleep_time = REQUEST_SLEEP * random.random()
                            logging.info("---- sleeping " + str(sleep_time) + " seconds...")
                            if HTTP_PROXY_LIST is not None:
                                logging.info("---- Currently using " + str(len(HTTP_PROXY_LIST)) + " proxies.")
                            time.sleep(sleep_time) # be nice
                            if listing.ws_get_room_info(FLAGS_ADD):
                                room_count += 1
                    except AttributeError as ae:
                        logger.error("Attribute error: marking room as deleted.")
                        listing.save_as_deleted()
                    except Exception as e:
                        logger.error("Error in fill_loop_by_room:" + str(type(e)))
                        raise
            else:
                # add in all listings from previous surveys of this search area
                #TODO: do this as an INSERT from SELECT to avoid the round trip
                conn = connect()
                sql_insert = """
                    insert into room (room_id, survey_id)
                    select distinct r.room_id, %s
                    from room r, survey s, search_area sa
                    where r.survey_id = s.survey_id
                    and s.search_area_id = sa.search_area_id
                    and sa.search_area_id = %s
                    and s.survey_id < %s
                    and r.room_id not in (
                        select room_id from room
                        where survey_id = %s
                        )
                    """
                cur = conn.cursor()
                cur.execute(sql_insert, (self.survey_id,
                    self.search_area_id, self.survey_id, self.survey_id,))
                cur.close()
                conn.commit()

                # Loop over neighborhoods or zipcode
                if search_by == SEARCH_BY_ZIPCODE:
                    zipcodes = db_get_zipcodes_from_search_area(self.search_area_id)
                    for room_type in ( "Private room", "Entire home/apt",):
                        logger.debug("Searching for %(rt)s by zipcode" % {"rt": room_type})
                        self.__search_loop_zipcodes(zipcodes, room_type, flag)
                else:
                    neighborhoods = db_get_neighborhoods_from_search_area(self.search_area_id)
                    for room_type in ( "Private room", "Entire home/apt", "Shared room",):
                        logger.debug("Searching for %(rt)s by neighborhood" % {"rt": room_type})
                        if len(neighborhoods) > 0:
                            self.__search_loop_neighborhoods(neighborhoods, room_type, flag)
                        else:
                            self.__search_neighborhood(None, room_type, flag)
        except KeyboardInterrupt:
            raise
        except Exception:
            raise
    def log_progress(self, room_type, neighborhood_id,
                               guests, page_number, has_rooms):
        try:
            page_info = (self.survey_id, room_type, neighborhood_id, 
                    guests, page_number, has_rooms)
            logger.debug("Survey search page: " + str(page_info))
            sql = """
            insert into survey_progress_log (survey_id, room_type, neighborhood_id,
            guests, page_number, has_rooms)
            values (%s, %s, %s, %s, %s, %s)
            """
            conn = connect()
            cur = conn.cursor()
            cur.execute(sql, page_info)
            cur.close()
            conn.commit()
            logger.info("Logging survey search page for neighborhood " + str(neighborhood_id))
            return True
        except psycopg2.Error as pge:
            logger.error(pge.pgerror)
            cur.close()
            conn.rollback()
            return False
        except Exception:
            logger.error("Save survey search page failed")
            return False


    def __set_search_area(self):
        try:
            conn = connect()
            cur = conn.cursor()
            cur.execute("""
                select sa.search_area_id, sa.name
                from search_area sa join survey s
                on sa.search_area_id = s.search_area_id
                where s.survey_id = %s""", (self.survey_id,))
            (self.search_area_id, self.search_area_name) = cur.fetchone()
            cur.close()
        except KeyboardInterrupt:
            cur.close()
            raise
        except Exception:
            cur.close()
            logger.error("No search area for survey_id " + str(survey.survey_id))
            raise
    def __search_loop_zipcodes(self, zipcodes, room_type, flag):
        try:
            for zipcode in zipcodes:
                search_zipcode(str(zipcode), room_type, survey_id,
                                    flag, search_area_name)
        except Exception:
            raise

    def __search_loop_neighborhoods(self, neighborhoods, room_type, flag):
        try:
            for neighborhood in neighborhoods:
                search_neighborhood(neighborhood, room_type, survey_id,
                                    flag, search_area_name)
        except Exception:
            raise

    def __search_neighborhood(self, neighborhood, room_type, flag):
        try:
            if room_type in ("Private room", "Shared room"):
                max_guests = 4
            else:
                max_guests = SEARCH_MAX_GUESTS
            for guests in range(1, max_guests):
                logger.debug("Searching for %(g)i guests" % {"g": guests})
                for page_number in range(1, SEARCH_MAX_PAGES):
                    if flag != FLAGS_PRINT:
                        # for FLAGS_PRINT, fetch one page and print it
                        count = page_has_been_retrieved(
                            self.survey_id, room_type,
                            neighborhood, guests, page_number, SEARCH_BY_NEIGHBORHOOD)
                        if count == 1:
                            logger.debug("\t...search page has been visited previously")
                            continue
                        elif count == 0:
                            logger.debug("\t...search page has been visited previously")
                            break
                        else:
                            logger.debug("\t...visiting search page")
                    room_count = ws_get_search_page_info(
                        self,
                        room_type,
                        neighborhood,
                        guests,
                        page_number,
                        flag)
                    if room_count <= 0:
                        break
                    if flag == FLAGS_PRINT:
                        return
        except Exception:
            raise
    
# ==============================================================================
# End of class Survey
# ==============================================================================

def list_search_area_info(search_area):
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute("""
                select search_area_id
                from search_area where name=%s
                """, (search_area,))
        result_set = cur.fetchall()
        cur.close()
        count = len(result_set)
        if count == 1:
            print("\nThere is one search area called",
                  str(search_area),
                  "in the database.")
        elif count > 1:
            print("\nThere are", str(count),
                  "cities called", str(search_area),
                  "in the database.")
        elif count < 1:
            print("\nThere are no cities called",
                  str(search_area),
                  "in the database.")
            sys.exit()
        sql_neighborhood = """select count(*) from neighborhood
        where search_area_id = %s"""
        sql_search_area = """select count(*) from search_area
        where search_area_id = %s"""
        for result in result_set:
            search_area_id = result[0]
            cur = conn.cursor()
            cur.execute(sql_neighborhood, (search_area_id,))
            count = cur.fetchone()[0]
            cur.close()
            print("\t" + str(count) + " neighborhoods.")
            cur = conn.cursor()
            cur.execute(sql_search_area, (search_area_id,))
            count = cur.fetchone()[0]
            cur.close()
            print("\t" + str(count) + " Airbnb cities.")
    except psycopg2.Error as pge:
        logger.error(pge.pgerror)
        logger.error("Error code " + pge.pgcode)
        logger.error("Diagnostics " + pge.diag.message_primary)
        cur.close()
        conn.rollback()
        raise
    except Exception:
        logger.error("Failed to list search area info")
        raise


def list_surveys():
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute("""
            select survey_id, to_char(survey_date, 'YYYY-Mon-DD'), survey_description, search_area_id
            from survey
            order by survey_id asc""")
        result_set = cur.fetchall()
        if len(result_set) > 0:
            template = "| {0:3} | {1:>12} | {2:>30} | {3:3} |"
            print(template.format("ID", "Date", "Description", "SA"))
            for survey in result_set:
                (id, survey_date, desc, sa_id) = survey
                print(template.format(id, survey_date, desc, sa_id))
    except Exception:
        logger.error("Cannot list surveys.")
        raise

def db_ping():
    try:
        conn = connect()
        if conn is not None:
            print("Connection test succeeded")
        else:
            print("Connection test failed")
    except Exception:
        logger.exception("Connection test failed")


def db_add_survey(search_area):
    try:
        conn = connect()
        cur = conn.cursor()
        # Add an entry into the survey table, and get the survey_id
        sql = """
        insert into survey (survey_description, search_area_id)
        select (name || ' (' || current_date || ')') as survey_description,
        search_area_id
        from search_area
        where name = %s
        returning survey_id"""
        cur.execute(sql, (search_area,))
        survey_id = cur.fetchone()[0]

        # Get and print the survey entry
        cur.execute("""select survey_id, survey_date,
        survey_description, search_area_id
        from survey where survey_id = %s""", (survey_id,))
        (survey_id,
         survey_date,
         survey_description,
         search_area_id) = cur.fetchone()
        conn.commit()
        cur.close()
        print("\nSurvey added:\n"
              + "\n\tsurvey_id=" + str(survey_id)
              + "\n\tsurvey_date=" + str(survey_date)
              + "\n\tsurvey_description=" + survey_description
              + "\n\tsearch_area_id=" + str(search_area_id))
    except Exception:
        logger.error("Failed to add survey for " + search_area)
        raise


def db_get_zipcodes_from_search_area(search_area_id):
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute("""
        select zip
        from zipcode_us z, search_area sa
        where search_area_id = %s
        and sa.name = z.county
        """, (search_area_id,))
        logger.info("X")
        zipcodes = []
        while True:
            row = cur.fetchone()
            if row is None:
                break
            zipcodes.append(row[0])
        cur.close()
        return zipcodes
    except Exception:
        logger.error("Failed to retrieve zipcodes for search_area"
                     + str(search_area_id))
        raise


def db_get_neighborhoods_from_search_area(search_area_id):
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute("""
            select name
            from neighborhood
            where search_area_id =  %s
            order by name""", (search_area_id,))
        neighborhoods = []
        while True:
            row = cur.fetchone()
            if row is None:
                break
            neighborhoods.append(row[0])
        cur.close()
        return neighborhoods
    except Exception:
        logger.error("Failed to retrieve neighborhoods from "
                     + str(search_area_id))
        raise


def db_get_search_area_info_from_db(search_area):
    try:
        # get city_id
        conn = connect()
        cur = conn.cursor()
        cur.execute("""
            select search_area_id
            from search_area
            where name = :search_area_name
            """, {"search_area_name": search_area})
        search_area_id = cur.fetchone()[0]
        print("\nFound search_area", search_area,
              ": search_area_id =", str(search_area_id))

        # get cities
        cur.execute("""select name
                       from city
                       where search_area_id = :search_area_id
                    """,
                    {"search_area_id": search_area_id})
        cities = []
        while True:
            row = cur.fetchone()
            if row is None:
                break
            cities.append(row[0])

        # get neighborhoods
        cur.execute("""
            select name
            from neighborhood
            where search_area_id =  :search_area_id
            """, {"search_area_id": search_area_id})
        neighborhoods = []
        while True:
            row = cur.fetchone()
            if row is None:
                break
            neighborhoods.append(row[0])

        cur.close()
        return (cities, neighborhoods)
    except Exception:
        logger.error("Error getting search area info from db")
        raise


def db_get_room_to_fill():
    try:
        sql = """
        select room_id, survey_id
        from room
        where price is null
        and (deleted = 0 or deleted is null)
        order by random()
        limit 1
        """
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        (room_id, survey_id) = cur.fetchone()
        listing = Listing(room_id, survey_id)
        cur.close()
        return listing
    except TypeError:
        logger.info("-- Finishing: no unfilled rooms in database --")
        conn = None
        return (None, None)
    except Exception:
        logger.error("Error retrieving room to fill from db")
        conn = None
        raise


def db_get_neighborhood_id(survey_id, neighborhood):
    try:
        sql = """
        select neighborhood_id
        from neighborhood nb,
            search_area sa,
            survey s
        where nb.search_area_id = sa.search_area_id
        and sa.search_area_id = s.search_area_id
        and s.survey_id = %s
        and nb.name = %s
        """
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql, (survey_id, neighborhood, ))
        neighborhood_id = cur.fetchone()[0]
        cur.close()
        conn.commit()
        return neighborhood_id
    except psycopg2.Error as pge:
        raise
    except Exception:
        return None


def ws_get_city_info(city, flag):
    try:
        url = URL_SEARCH_ROOT + city
        page = ws_get_page(url)
        if page is False:
            return False
        tree = html.fromstring(page)
        try:
            citylist = tree.xpath(
                "//input[@name='location']/@value")
            neighborhoods = tree.xpath(
                "//input[@name='neighborhood']/@value")
            if flag == FLAGS_PRINT:
                print("\n", citylist[0])
                print("Neighborhoods:")
                for neighborhood in neighborhoods:
                    print("\t", neighborhood)
            elif flag == FLAGS_ADD:
                if len(citylist) > 0:
                    conn = connect()
                    cur = conn.cursor()
                    # check if it exists
                    sql_check = """
                        select name
                        from search_area
                        where name = %s"""
                    cur.execute(sql_check, (citylist[0],))
                    if cur.fetchone() is not None:
                        logger.info("City already exists: " + citylist[0])
                        return
                    sql_search_area = """insert
                                into search_area (name)
                                values (%s)"""
                    cur.execute(sql_search_area, (citylist[0],))
                    #city_id = cur.lastrowid
                    sql_identity = """select currval('search_area_search_area_id_seq')"""
                    cur.execute(sql_identity, ())
                    search_area_id = cur.fetchone()[0]
                    sql_city = """insert
                            into city (name, search_area_id)
                            values (%s,%s)"""
                    cur.execute(sql_city, (city, search_area_id,))
                    logger.info("Added city " + city)
                    logger.debug(str(len(neighborhoods)) + " neighborhoods")
                if len(neighborhoods) > 0:
                    sql_neighborhood = """
                        insert
                        into neighborhood(name, search_area_id)
                        values(%s, %s)
                        """
                    for neighborhood in neighborhoods:
                        cur.execute(sql_neighborhood, (neighborhood,
                                                       search_area_id,))
                        logger.info("Added neighborhood " + neighborhood)
                else:
                    logger.info("No neighborhoods found for " + city)
                conn.commit()
        except UnicodeEncodeError:
            #if sys.version_info >= (3,):
            #    logger.info(s.encode('utf8').decode(sys.stdout.encoding))
            #else:
            #    logger.info(s.encode('utf8'))
            # unhandled at the moment
            pass
        except Exception:
            logger.error("Error collecting city and neighborhood information")
            raise
    except Exception:
        logger.error("Error getting city info from website")
        raise

def ws_airbnb_is_live():
    try:
        urllib.request.urlopen(URL_ROOT)
        return True         # URL Exist
    except ValueError as ex:
        return False        # URL not well formatted
    except urllib.URLError as ex:
        return False        # URL don't seem to be alive


def ws_get_page(url):
    # chrome gets the JavaScript-loaded content as well
    # see http://webscraping.com/blog/Scraping-JavaScript-webpages-with-webkit/
    # r = Render(url)
    # page = r.frame.toHtml()
    try:
        attempt = 0
        for attempt in range(MAX_CONNECTION_ATTEMPTS):
            try:
                # If there is a list of proxies supplied, use it
                http_proxy = None
                if HTTP_PROXY_LIST is not None:
                    http_proxy = random.choice(HTTP_PROXY_LIST)
                    proxy_handler = urllib.request.ProxyHandler({
                        'http': http_proxy,
                        'https': http_proxy,
                    })
                    opener = urllib.request.build_opener(proxy_handler)
                    urllib.request.install_opener(opener)
                # Now make the request
                req = urllib.request.Request(url,
                    headers={'User-Agent': 'Mozilla/5.0'}
                )
                response = urllib.request.urlopen(req, timeout=HTTP_TIMEOUT)
                page = response.read()
                break
            except KeyboardInterrupt:
                sys.exit()
            except urllib.error.HTTPError as he:
                if http_proxy is None:
                    logger.error("HTTP error " + str(he.code))
                    # fill the proxy list again, and wait a long time, then restart
                    init()
                    logging.info("Waiting to re-initialize: "
                        + str(RE_INIT_SLEEP_TIME)
                        + " seconds...")
                    time.sleep(RE_INIT_SLEEP_TIME) # be nice
                    return False
                logger.error("HTTP error " + str(he.code) + " for proxy " + http_proxy)
                if he.code == 503:
                    if random.random() < 0.5:
                        logging.error("Removing " + http_proxy + " from HTTP_PROXY_LIST")
                        HTTP_PROXY_LIST.remove(http_proxy)
                        if len(HTTP_PROXY_LIST) < 1:
                            logging.error("No proxies left in the list. Re-initializing.")
                            # fill the proxy list again, and wait a long time, then restart
                            init()
                            time.sleep(RE_INIT_SLEEP_TIME) # be nice
                            return False
                if attempt >= (MAX_CONNECTION_ATTEMPTS - 1):
                    logger.error("Probable connectivity problem retrieving " +
                                 "web page " + url)
                    if ws_airbnb_is_live():
                        return False
                    else:
                        raise
            except NameError as ne:
                logger.error("NameError: " + ne.message)
                if attempt >= (MAX_CONNECTION_ATTEMPTS - 1):
                    logger.error("Probable connectivity problem retrieving " +
                                 "web page " + url)
                else:
                    pass
            except Exception as e:
                logger.error("Failed to retrieve web page " + url)
                logger.error("Exception type: " + type(exception).__name__)
                if attempt >= (MAX_CONNECTION_ATTEMPTS - 1):
                    logger.error("Probable connectivity problem retrieving " +
                                 "web page " + url)
                    if ws_airbnb_is_live():
                        return False
                    else:
                        raise
        return page
    except urllib.error.HTTPError:
        raise
    except urllib.error.URLError:
        logger.error("URLError retrieving page")
        raise
    except NameError as ne:
        logger.error("NameError retrieving page")
        return False
    except AttributeError as ae:
        logger.error("AttributeError retrieving page")
        return False
    except Exception as e:
        logger.error("Exception retrieving page: " + str(type(e)))
        raise


def ws_get_search_page_info_zipcode(survey, search_area_name, room_type,
                            zipcode, guests, page_number, flag):
    try:
        logger.info(
            room_type + ", " +
            str(zipcode) + ", " +
            str(guests) + " guests, " +
            "page " + str(page_number)  )
        url = search_page_url(zipcode, guests,
                              None, room_type,
                              page_number)
        sleep_time = REQUEST_SLEEP * random.random()
        logging.info("-- sleeping " + str(sleep_time) + " seconds...")
        time.sleep(sleep_time) # be nice
        page = ws_get_page(url)
        if page is False:
            return 0
        tree = html.fromstring(page)
        room_elements = tree.xpath(
            "//div[@class='listing']/@data-id"
        )
        logger.debug("Found " + str(len(room_elements)) + " rooms.")
        room_count = len(room_elements)
        if room_count > 0:
            has_rooms = 1
        else:
            has_rooms = 0
        if flag == FLAGS_ADD:
            survey.log_progress(room_type, zipcode, guests,
                                       page_number, has_rooms)
        if room_count > 0:
            for room_element in room_elements:
                room_id = int(room_element)
                if room_id is not None:
                    listing = Listing(room_id, survey.survey_id)
                    if flag == FLAGS_ADD:
                        listing.save(FLAGS_INSERT_NO_REPLACE)
                    elif flag == FLAGS_PRINT:
                        print(room_type, listing.room_id)
        else:
            logger.info("No rooms found")
        return room_count
    except UnicodeEncodeError:
        logger.error("UnicodeEncodeError: you may want to  set PYTHONIOENCODING=utf-8")
        #if sys.version_info >= (3,):
        #    logger.info(s.encode('utf8').decode(sys.stdout.encoding))
        #else:
        #    logger.info(s.encode('utf8'))
        # unhandled at the moment
        pass
    except Exception:
        raise

def ws_get_search_page_info(survey, room_type,
                    neighborhood, guests, page_number, flag):
    try:
        logger.info(
            room_type + ", " +
            str(neighborhood) + ", " +
            str(guests) + " guests, " +
            "page " + str(page_number)  )
        url = search_page_url(survey.search_area_name, guests,
                              neighborhood, room_type,
                              page_number)
        sleep_time = REQUEST_SLEEP * random.random()
        logging.info("-- sleeping " + str(sleep_time) + " seconds...")
        time.sleep(sleep_time) # be nice
        page = ws_get_page(url)
        if page is False:
            return 0
        tree = html.fromstring(page)
        room_elements = tree.xpath(
            "//div[@class='listing']/@data-id"
        )
        logger.debug("Found " + str(len(room_elements)) + " rooms.")
        room_count = len(room_elements)
        if room_count > 0:
            has_rooms = 1
        else:
            has_rooms = 0
        if flag == FLAGS_ADD:
            neighborhood_id = db_get_neighborhood_id(survey.survey_id, neighborhood)
            survey.log_progress(room_type, neighborhood_id, guests,
                                       page_number, has_rooms)
        if room_count > 0:
            for room_element in room_elements:
                room_id = int(room_element)
                if room_id is not None:
                    listing = Listing(room_id, survey.survey_id, room_type)
                    if flag == FLAGS_ADD:
                        listing.save(FLAGS_INSERT_NO_REPLACE)
                        #db_save_room_info(room_info, FLAGS_INSERT_NO_REPLACE)
                    elif flag == FLAGS_PRINT:
                        print(room_type, listing.room_id)
        else:
            logger.info("No rooms found")
        return room_count
    except UnicodeEncodeError:
        logger.error("UnicodeEncodeError: you may want to  set PYTHONIOENCODING=utf-8")
        #if sys.version_info >= (3,):
        #    logger.info(s.encode('utf8').decode(sys.stdout.encoding))
        #else:
        #    logger.info(s.encode('utf8'))
        # unhandled at the moment
        pass
    except Exception:
        raise


def display_room(room_id):
    webbrowser.open(URL_ROOM_ROOT + str(room_id))


def display_host(host_id):
    webbrowser.open(URL_HOST_ROOT + str(host_id))


def fill_loop_by_room():
    room_count = 0
    while room_count < FILL_MAX_ROOM_COUNT:
        try:
            room_count += 1
            listing = db_get_room_to_fill()
            if listing.room_id is None:
                break
            else:
                sleep_time = REQUEST_SLEEP * random.random()
                logging.info("---- sleeping " + str(sleep_time) + " seconds...")
                if HTTP_PROXY_LIST is not None:
                    logging.info("---- Currently using " + str(len(HTTP_PROXY_LIST)) + " proxies.")
                time.sleep(sleep_time) # be nice
                if listing.ws_get_room_info(FLAGS_ADD):
                    pass
                else: #Airbnb now seems to return nothing if a room has gone
                    listing.save_as_deleted()
        except urllib.error.HTTPError as he:
            if he.code == 503:
                # failed to get a web page. Try again
                pass
        except AttributeError as ae:
            logger.error("Attribute error: marking room as deleted.")
            listing.save_as_deleted()
        except Exception as e:
            logger.error("Error in fill_loop_by_room:" + str(type(e)))
            raise


def page_has_been_retrieved(survey_id, room_type, neighborhood_or_zipcode, guests,
                            page_number, search_by):
    """
    Returns 1 if the page has been retrieved previously and has rooms
    Returns 0 if the page has been retrieved previously and has no rooms
    Returns -1 if the page has not been retrieved previously
    """
    conn = connect()
    cur = conn.cursor()
    has_rooms = 0
    try:
        if search_by==SEARCH_BY_NEIGHBORHOOD:
            neighborhood = neighborhood_or_zipcode
            # TODO: Currently fails when there are no neighborhoods
            if neighborhood is None:
                has_rooms = -1
            else:
                params =  (survey_id, room_type, neighborhood, guests, page_number,)
                logger.debug("Params: " + str(params))
                sql = """
                select spl.has_rooms
                from survey_progress_log spl
                join neighborhood nb
                on spl.neighborhood_id = nb.neighborhood_id
                where survey_id = %s
                and room_type = %s
                and nb.name = %s
                and guests = %s
                and page_number = %s"""
                cur.execute(sql, params)
                has_rooms = cur.fetchone()[0]
                logger.debug("has_rooms = " + str(has_rooms) + " for neighborhood " + neighborhood)
        else: # SEARCH_BY_ZIPCODE
            zipcode = int(neighborhood_or_zipcode)
            params =  (survey_id, room_type, zipcode, guests, page_number,)
            logger.debug(params)
            sql = """
                select spl.has_rooms
                from survey_progress_log spl
                where survey_id = %s
                and room_type = %s
                and neighborhood_id = %s
                and guests = %s
                and page_number = %s"""
            cur.execute(sql, params)
            has_rooms = cur.fetchone()[0]
            logger.debug("has_rooms = " + str(has_rooms) + " for zipcode " + str(zipcode))
    except Exception:
        has_rooms = -1
        logger.exception("Exception in page_has_been_retrieved")
        logger.debug("page has not been retrieved previously")
    finally:
        cur.close()
        return has_rooms


def search_page_url(search_string, guests, neighborhood, room_type,
                    page_number):
    # search_string is either a search area name or a zipcode
    url_root = URL_SEARCH_ROOT + search_string
    url_suffix = "guests=" + str(guests)
    if neighborhood is not None:
        url_suffix += "&"
        url_suffix += urllib.parse.quote("neighborhoods[]")
        url_suffix += "="
    # Rome: Unicode wording, equal comparison failed
    # to convert both args to unicode (prob url_suffix
    # and urllib2.quote(neighborhood)
        url_suffix += urllib.parse.quote(neighborhood)
    url_suffix += "&"
    url_suffix += urllib.parse.quote("room_types[]")
    url_suffix += "="
    url_suffix += urllib.parse.quote(room_type)
    url_suffix += "&"
    url_suffix += "page=" + str(page_number)
    url = url_root + "?" + url_suffix
    return url


def search_zipcode(zipcode, room_type, survey_id,
                        flag, search_area_name):
    try:
        if room_type in ("Private room", "Shared room"):
            max_guests = 4
        else:
            max_guests = SEARCH_MAX_GUESTS
        for guests in range(1, max_guests):
            logger.debug("Searching for %(g)i guests" % {"g": guests})
            for page_number in range(1, SEARCH_MAX_PAGES):
                if flag != FLAGS_PRINT:
                    # for FLAGS_PRINT, fetch one page and print it
                    # this efficiency check can be implemented later
                    count = page_has_been_retrieved(
                        survey_id, room_type,
                        str(zipcode), guests, page_number, SEARCH_BY_ZIPCODE)
                    if count == 1:
                        logger.debug("\t...search page has been visited previously")
                        continue
                    elif count == 0:
                        logger.debug("\t...search page has been visited previously")
                        break
                    else:
                        logger.debug("\t...visiting search page")
                room_count = ws_get_search_page_info_zipcode(
                    survey,
                    search_area_name,
                    room_type,
                    zipcode,
                    guests,
                    page_number,
                    flag)
                if room_count <= 0:
                    break
                if flag == FLAGS_PRINT:
                    return
    except Exception:
        raise



def main():
    init()
    parser = \
        argparse.ArgumentParser(
            description='Manage a database of Airbnb listings.',
            usage='%(prog)s [options]')
    # Only one argument!
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-asa', '--addsearcharea',
                       metavar='search_area', action='store', default=False,
                       help="""get and save the name and neighborhoods
                       for search area (city)""")
    group.add_argument('-asv', '--addsurvey',
                       metavar='search_area', type=str,
                       help="""add a survey entry to the database,
                       for search_area""")
    group.add_argument('-dbi', '--dbinit',
                       action='store_true', default=False,
                       help='Initialize the database file')
    group.add_argument('-dbp', '--dbping',
                       action='store_true', default=False,
                       help='Test the database connection')
    group.add_argument('-dh', '--displayhost',
                       metavar='host_id', type=int,
                       help='display web page for host_id in browser')
    group.add_argument('-dr', '--displayroom',
                       metavar='room_id', type=int,
                       help='display web page for room_id in browser')
    group.add_argument('-f', '--fill',
                       action='store_true', default=False,
                       help='fill in details for room_ids collected with -s')
    group.add_argument('-g', '--geolocate',
                       metavar='survey_id', type=int,
                       help='geolocate entries in room table for the given survey')
    group.add_argument('-lsa', '--listsearcharea',
                       metavar='search_area', type=str,
                       help="""list information about this search area
                       from the database""")
    group.add_argument('-lr', '--listroom',
                       metavar='room_id', type=int,
                       help='list information about room_id from the database')
    group.add_argument('-ls', '--listsurveys',
                       action='store_true', default=False,
                       help='list the surveys in the database')
    group.add_argument('-psa', '--printsearcharea',
                       metavar='search_area', action='store', default=False,
                       help="""print the name and neighborhoods for
                       search area (city) from the Airbnb web site""")
    group.add_argument('-pr', '--printroom',
                       metavar='room_id', type=int,
                       help="""print room_id information
                       from the Airbnb web site""")
    group.add_argument('-ps', '--printsearch',
                       metavar='survey_id', type=int,
                       help="""print first page of search information
                       for survey from the Airbnb web site""")
    group.add_argument('-psz', '--printsearch_by_zipcode',
                       metavar='survey_id', type=int,
                       help="""print first page of search information
                       for survey from the Airbnb web site, by zipcode""")
    group.add_argument('-s', '--search',
                       metavar='survey_id', type=int,
                       help='search for rooms using survey survey_id')
    group.add_argument('-sz', '--search_by_zipcode',
                       metavar='survey_id', type=int,
                       help='search for rooms using survey survey_id, by zipcode')
    group.add_argument('-v', '--version',
                       action='version',
                       version='%(prog)s, version SCRIPT_VERSION_NUMBER')
    group.add_argument('-?', action='help')

    args = parser.parse_args()

    try:
        if args.search:
            survey = Survey(args.search)
            survey.search(FLAGS_ADD, SEARCH_BY_NEIGHBORHOOD)
        elif args.search_by_zipcode:
            survey = Survey(args.search_by_zipcode)
            survey.search(FLAGS_ADD, SEARCH_BY_ZIPCODE)
        elif args.fill:
            fill_loop_by_room()
        elif args.addsearcharea:
            ws_get_city_info(args.addsearcharea, FLAGS_ADD)
        elif args.addsurvey:
            db_add_survey(args.addsurvey)
        elif args.dbinit:
            db_init()
        elif args.dbping:
            db_ping()
        elif args.displayhost:
            display_host(args.displayhost)
        elif args.displayroom:
            display_room(args.displayroom)
        elif args.geolocate:
            geolocate(args.geolocate)
        elif args.listsearcharea:
            list_search_area_info(args.listsearcharea)
        elif args.listroom:
            listing = Listing(args.listroom, None)
            listing.print_from_db()
        elif args.listsurveys:
            list_surveys()
        elif args.printsearcharea:
            ws_get_city_info(args.printsearcharea, FLAGS_PRINT)
        elif args.printroom:
            listing = Listing(args.printroom, None)
            listing.ws_get_room_info(FLAGS_PRINT)
        elif args.printsearch:
            survey = Survey(args.printsearch)
            survey.search(FLAGS_PRINT, SEARCH_BY_NEIGHBORHOOD)
        elif args.printsearch_by_zipcode:
            survey = Survey(args.printsearch_by_zipcode)
            survey.search(FLAGS_PRINT, SEARCH_BY_ZIPCODE)
        else:
            parser.print_help()
    except KeyboardInterrupt:
        sys.exit()
    except SystemExit:
        # sys.exit() called: don't log a stack trace
        pass
    except Exception:
        logger.exception("Top level exception handler: quitting.")
        sys.exit(0)

if __name__ == "__main__":
    main()
