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
import requests
from lxml import html
from datetime import date, datetime
import psycopg2
import psycopg2.errorcodes
import webbrowser
import json
from airbnb_config import ABConfig

# ============================================================================
# CONSTANTS
# ============================================================================

# Survey characteristics: filled in from <username>.config
SEARCH_RECTANGLE_EDGE_BLUR = 0.1
SEARCH_BY_NEIGHBORHOOD = 'neighborhood'  # default
SEARCH_BY_ZIPCODE = 'zipcode'
SEARCH_BY_BOUNDING_BOX = 'bounding box'
SEARCH_LISTINGS_ON_FULL_PAGE = 18

# URLs (fixed)
URL_ROOT = "http://www.airbnb.com/"
URL_ROOM_ROOT = URL_ROOT + "rooms/"
URL_HOST_ROOT = URL_ROOT + "users/show/"
URL_SEARCH_ROOT = URL_ROOT + "s/"
URL_API_SEARCH_ROOT = URL_ROOT + "search/search_results"

# Other internal constants
SEARCH_AREA_GLOBAL = "UNKNOWN"  # special case: sample listings globally
FLAGS_ADD = 1
FLAGS_PRINT = 9
FLAGS_INSERT_REPLACE = True
FLAGS_INSERT_NO_REPLACE = False

# Script version
# 2.6 adds a bounding box search
# 2.5 is a bit of a rewrite: classes for Listing and Survey, and requests lib
# 2.3 released Jan 12, 2015, to handle a web site update
SCRIPT_VERSION_NUMBER = 2.6

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Suppress informational logging from requests module
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

ch_formatter = logging.Formatter('%(levelname)-8s%(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(ch_formatter)
logger.addHandler(console_handler)

# logging: set log file name, format, and level
filelog_formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s%(message)s')
logfile = str(datetime.now())
logfile = logfile.replace(":", "_")
logfile = logfile.replace(" ", "_")
logfile = logfile.replace(".", "_")
logfile += ".log"
filelog_handler = logging.FileHandler(logfile, encoding="utf-8")
filelog_handler.setFormatter(filelog_formatter)
logger.addHandler(filelog_handler)


class Listing():
    """
    # Listing represents an Airbnb room_id, as captured at a moment in time.
    # room_id, survey_id is the primary key.
    # Occasionally, a survey_id = None will happen, but for retrieving data
    # straight from the web site, and not stored in the database.
    """
    def __init__(self, config, room_id, survey_id, room_type=None):
        self.config = config
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
        self.deleted = None
        self.minstay = None
        self.latitude = None
        self.longitude = None
        self.survey_id = survey_id
        #  extra fields added from search json:
        # coworker_hosted (bool)
        self.coworker_hosted = None
        # extra_host_languages (list)
        self.extra_host_languages = None
        # name (str)
        self.name = None
        # property_type (str)
        self.property_type = None
        # currency (str)
        self.currency = None
        # rate_type (str) - "nightly" or other?
        self.rate_type = None
        """ """

    def status_check(self):
        status = True  # OK
        unassigned_values = {key: value
                             for key, value in vars(self).items()
                             if not key.startswith('__') and
                             not callable(key) and
                             value is None
                             }
        if len(unassigned_values) > 9:  # just a value indicating deleted
            logger.info("Room " + str(self.room_id) + ": marked deleted")
            status = False  # probably deleted
            self.deleted = 1
        else:
            for key, val in unassigned_values.items():
                if (key == "overall_satisfaction" and "reviews" not in
                        unassigned_values):
                    if val is None and self.reviews > 2:
                        logger.debug("Room " + str(self.room_id) + ": No value for " + key)
                elif val is None:
                    logger.debug("Room " + str(self.room_id) + ": No value for " + key)
        return status

    def get_columns(self):
        """
        Hack: callable(attr) includes methods with (self) as argument.
        Need to find a way to avoid these.
        This hack does also provide the proper order, which matters
        """
        # columns = [attr for attr in dir(self) if not
        # callable(attr) and not attr.startswith("__")]
        columns = ("room_id", "host_id", "room_type", "country",
                   "city", "neighborhood", "address", "reviews",
                   "overall_satisfaction", "accommodates", "bedrooms",
                   "bathrooms", "price", "deleted", "minstay",
                   "latitude", "longitude", "survey_id", "last_modified",)
        return columns

    def save_as_deleted(self):
        try:
            logger.debug("Marking room deleted: " + str(self.room_id))
            if self.survey_id is None:
                return
            conn = self.config.connect()
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
        """
        Save a listing in the database. Delegates to lower-level methods
        to do the actual database operations.
        Return values:
            True: listing is saved in the database
            False: listing already existed
        """
        try:
            rowcount = -1
            if self.deleted == 1:
                self.save_as_deleted()
            else:
                if insert_replace_flag == FLAGS_INSERT_REPLACE:
                    rowcount = self.__update()
                if (rowcount == 0 or
                        insert_replace_flag == FLAGS_INSERT_NO_REPLACE):
                    try:
                        self.__insert()
                        return True
                    except psycopg2.IntegrityError:
                        logger.debug("Room " + str(self.room_id) + ": already collected")
                        return False
        except psycopg2.OperationalError:
            # connection closed
            del(self.config.connection)
            logger.error("Operational error (connection closed): resuming")
            del(self.config.connection)
        except psycopg2.DatabaseError as de:
            self.config.connection.conn.rollback()
            logger.erro(psycopg2.errorcodes.lookup(de.pgcode[:2]))
            logger.error("Database error: resuming")
            del(self.config.connection)
        except psycopg2.InterfaceError:
            # connection closed
            logger.error("Interface error: resuming")
            del(self.config.connection)
        except psycopg2.Error as pge:
            # database error: rollback operations and resume
            self.config.connection.conn.rollback()
            logger.error("Database error: " + str(self.room_id))
            logger.error("Diagnostics " + pge.diag.message_primary)
        except KeyboardInterrupt:
            self.config.connection.rollback()
            raise
        except UnicodeEncodeError as uee:
            logger.error("UnicodeEncodeError Exception at " +
                         str(uee.object[uee.start:uee.end]))
            raise
        except ValueError:
            logger.error("ValueError for room_id = " + str(self.room_id))
        except AttributeError:
            logger.error("AttributeError")
            raise
        except Exception:
            self.config.connection.rollback()
            logger.error("Exception saving room")
            raise

    def print_from_web_site(self):
        """ What is says """
        try:
            print_string = "Room info:"
            print_string += "\n\troom_id:\t" + str(self.room_id)
            print_string += "\n\tsurvey_id:\t" + str(self.survey_id)
            print_string += "\n\thost_id:\t" + str(self.host_id)
            print_string += "\n\troom_type:\t" + str(self.room_type)
            print_string += "\n\tcountry:\t" + str(self.country)
            print_string += "\n\tcity:\t\t" + str(self.city)
            print_string += "\n\tneighborhood:\t" + str(self.neighborhood)
            print_string += "\n\taddress:\t" + str(self.address)
            print_string += "\n\treviews:\t" + str(self.reviews)
            print_string += "\n\toverall_satisfaction:\t"
            print_string += str(self.overall_satisfaction)
            print_string += "\n\taccommodates:\t" + str(self.accommodates)
            print_string += "\n\tbedrooms:\t" + str(self.bedrooms)
            print_string += "\n\tbathrooms:\t" + str(self.bathrooms)
            print_string += "\n\tprice:\t\t" + str(self.price)
            print_string += "\n\tdeleted:\t" + str(self.deleted)
            print_string += "\n\tlatitude:\t" + str(self.latitude)
            print_string += "\n\tlongitude:\t" + str(self.longitude)
            print_string += "\n\tminstay:\t" + str(self.minstay)
            print_string += "\n\tcoworker_hosted:\t" + str(self.coworker_hosted)
            print_string += "\n\tlanguages:\t" + str(self.extra_host_languages)
            print_string += "\n\tproperty_type:\t" + str(self.property_type)
            print(print_string)
        except Exception:
            raise

    def print_from_db(self):
        """ What it says """
        try:
            columns = self.get_columns()
            sql = "select room_id"
            for column in columns[1:]:
                sql += ", " + column
            sql += " from room where room_id = %s"
            conn = self.config.connect()
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
                print("\nNo room", str(self.room_id), "in the database.\n")
                return False
            cur.close()
        except Exception:
            raise

    def ws_get_room_info(self, flag):
        """ Get the room properties from the web site """
        try:
            # initialization
            logger.info("-" * 70)
            logger.info("Room " + str(self.room_id) +
                        ": getting from Airbnb web site")
            room_url = URL_ROOM_ROOT + str(self.room_id)
            response = ws_request_with_repeats(self.config, room_url)
            if response is not None:
                page = response.text
                tree = html.fromstring(page)
                self.__get_room_info_from_tree(tree, flag)
                return True
            else:
                return False
        except KeyboardInterrupt:
            logger.error("Keyboard interrupt")
            raise
        except Exception as ex:
            logger.exception("Room " + str(self.room_id) +
                             ": failed to retrieve from web site.")
            logger.error("Exception: " + str(type(ex)))
            raise

    def __insert(self):
        """ Insert a room into the database. Raise an error if it fails """
        try:
            logger.debug("Values: ")
            logger.debug("\troom_id: {}".format(self.room_id))
            logger.debug("\thost_id: {}".format(self.host_id))
            conn = self.config.connect()
            cur = conn.cursor()
            sql = """
                insert into room (
                    room_id, host_id, room_type, country, city,
                    neighborhood, address, reviews, overall_satisfaction,
                    accommodates, bedrooms, bathrooms, price, deleted,
                    minstay, latitude, longitude, survey_id,
                    coworker_hosted, extra_host_languages, name,
                    property_type, currency, rate_type

                )
                """
            sql += """
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
                )"""
            insert_args = (
                self.room_id, self.host_id, self.room_type, self.country,
                self.city, self.neighborhood, self.address, self.reviews,
                self.overall_satisfaction, self.accommodates, self.bedrooms,
                self.bathrooms, self.price, self.deleted, self.minstay,
                self.latitude, self.longitude, self.survey_id,
                int(self.coworker_hosted), self.extra_host_languages, self.name,
                self.property_type, self.currency, self.rate_type
                )
            cur.execute(sql, insert_args)
            cur.close()
            conn.commit()
            logger.debug("Room " + str(self.room_id) + ": inserted")
            logger.debug("(lat, long) = ({lat:+.5f}, {lng:+.5f})".format(lat=self.latitude, lng=self.longitude))
        except psycopg2.IntegrityError:
            # logger.info("Room " + str(self.room_id) + ": insert failed")
            conn.rollback()
            cur.close()
            raise
        except:
            conn.rollback()
            raise

    def __update(self):
        """ Update a room in the database. Raise an error if it fails.
        Return number of rows affected."""
        try:
            rowcount = 0
            conn = self.config.connect()
            cur = conn.cursor()
            logger.debug("Updating...")
            sql = """
                update room
                set host_id = %s, room_type = %s,
                    country = %s, city = %s, neighborhood = %s,
                    address = %s, reviews = %s, overall_satisfaction = %s,
                    accommodates = %s, bedrooms = %s, bathrooms = %s,
                    price = %s, deleted = %s, last_modified = now()::timestamp,
                    minstay = %s, latitude = %s, longitude = %s,
                    coworker_hosted = %s, extra_host_languages = %s, name = %s,
                    property_type = %s, currency = %s, rate_type = %s
                where room_id = %s
                and survey_id = %s"""
            update_args = (
                self.host_id, self.room_type,
                self.country, self.city, self.neighborhood,
                self.address, self.reviews, self.overall_satisfaction,
                self.accommodates, self.bedrooms, self.bathrooms,
                self.price, self.deleted,
                self.minstay, self.latitude,
                self.longitude,
                self.coworker_hosted, self.extra_host_languages, self.name,
                self.property_type, self.currency, self.rate_type,
                self.room_id,
                self.survey_id,
                )
            logger.debug("Executing...")
            cur.execute(sql, update_args)
            rowcount = cur.rowcount
            logger.debug("Closing...")
            cur.close()
            conn.commit()
            logger.info("Room " + str(self.room_id) +
                        ": updated (" + str(rowcount) + ")")
            return rowcount
        except:
            # may want to handle connection close errors
            logger.warning("Exception in __update: raising")
            raise

    def __get_country(self, tree):
        try:
            temp = tree.xpath(
                "//meta[contains(@property,'airbedandbreakfast:country')]"
                "/@content"
                )
            if len(temp) > 0:
                self.country = temp[0]
        except:
            raise

    def __get_city(self, tree):
        try:
            temp = tree.xpath(
                "//meta[contains(@property,'airbedandbreakfast:city')]"
                "/@content"
                )
            if len(temp) > 0:
                self.city = temp[0]
        except:
            raise

    def __get_rating(self, tree):
        try:
            # 2016-04-10
            s = tree.xpath("//meta[@id='_bootstrap-listing']/@content")
            temp = tree.xpath(
                "//meta[contains(@property,'airbedandbreakfast:rating')]"
                "/@content"
                )
            if s is not None:
                j = json.loads(s[0])
                self.overall_satisfaction = j["listing"]["star_rating"]
            elif len(temp) > 0:
                self.overall_satisfaction = temp[0]
        except IndexError:
            return
        except:
            raise

    def __get_latitude(self, tree):
        try:
            temp = tree.xpath("//meta"
                              "[contains(@property,"
                              "'airbedandbreakfast:location:latitude')]"
                              "/@content")
            if len(temp) > 0:
                self.latitude = temp[0]
        except:
            raise

    def __get_longitude(self, tree):
        try:
            temp = tree.xpath(
                "//meta"
                "[contains(@property,'airbedandbreakfast:location:longitude')]"
                "/@content")
            if len(temp) > 0:
                self.longitude = temp[0]
        except:
            raise

    def __get_host_id(self, tree):
        try:
            # 2016-04-10
            s = tree.xpath("//meta[@id='_bootstrap-listing']/@content")
            temp = tree.xpath(
                "//div[@id='host-profile']"
                "//a[contains(@href,'/users/show')]"
                "/@href"
            )
            if s is not None:
                j = json.loads(s[0])
                self.host_id = j["listing"]["user"]["id"]
                return
            elif len(temp) > 0:
                host_id_element = temp[0]
                host_id_offset = len('/users/show/')
                self.host_id = int(host_id_element[host_id_offset:])
            else:
                temp = tree.xpath(
                    "//div[@id='user']"
                    "//a[contains(@href,'/users/show')]"
                    "/@href")
                if len(temp) > 0:
                    host_id_element = temp[0]
                    host_id_offset = len('/users/show/')
                    self.host_id = int(host_id_element[host_id_offset:])
        except IndexError:
            return
        except:
            raise

    def __get_room_type(self, tree):
        try:
            # -- room type --
            # new page format 2015-09-30?
            temp = tree.xpath(
                "//div[@class='col-md-6']"
                "/div/span[text()[contains(.,'Room type:')]]"
                "/../strong/text()"
                )
            if len(temp) > 0:
                self.room_type = temp[0].strip()
            else:
                # new page format 2014-12-26
                temp_entire = tree.xpath(
                    "//div[@id='summary']"
                    "//i[contains(concat(' ', @class, ' '),"
                    " ' icon-entire-place ')]"
                    )
                if len(temp_entire) > 0:
                    self.room_type = "Entire home/apt"
                temp_private = tree.xpath(
                    "//div[@id='summary']"
                    "//i[contains(concat(' ', @class, ' '),"
                    " ' icon-private-room ')]"
                    )
                if len(temp_private) > 0:
                    self.room_type = "Private room"
                temp_shared = tree.xpath(
                    "//div[@id='summary']"
                    "//i[contains(concat(' ', @class, ' '),"
                    " ' icon-shared-room ')]"
                    )
                if len(temp_shared) > 0:
                    self.room_type = "Shared room"
        except:
            raise

    def __get_neighborhood(self, tree):
        try:
            temp2 = tree.xpath(
                "//div[contains(@class,'rich-toggle')]/@data-address"
                )
            temp1 = tree.xpath("//table[@id='description_details']"
                               "//td[text()[contains(.,'Neighborhood:')]]"
                               "/following-sibling::td/descendant::text()")
            if len(temp2) > 0:
                temp = temp2[0].strip()
                self.neighborhood = temp[temp.find("(")+1:temp.find(")")]
            elif len(temp1) > 0:
                self.neighborhood = temp1[0].strip()
            if self.neighborhood is not None:
                self.neighborhood = self.neighborhood[:50]
        except:
            raise

    def __get_address(self, tree):
        try:
            temp = tree.xpath(
                "//div[contains(@class,'rich-toggle')]/@data-address"
                )
            if len(temp) > 0:
                temp = temp[0].strip()
                self.address = temp[:temp.find(",")]
            else:
                # try old page match
                temp = tree.xpath(
                    "//span[@id='display-address']"
                    "/@data-location"
                    )
                if len(temp) > 0:
                    self.address = temp[0]
        except:
            raise

    def __get_reviews(self, tree):
        try:
            # 2016-04-10
            s = tree.xpath("//meta[@id='_bootstrap-listing']/@content")
            # 2015-10-02
            temp2 = tree.xpath(
                "//div[@class='___iso-state___p3summarybundlejs']"
                "/@data-state"
                )
            if s is not None:
                j = json.loads(s[0])
                self.reviews = \
                    j["listing"]["review_details_interface"]["review_count"]
            elif len(temp2) == 1:
                summary = json.loads(temp2[0])
                self.reviews = summary["visibleReviewCount"]
            elif len(temp2) == 0:
                temp = tree.xpath(
                    "//div[@id='room']/div[@id='reviews']//h4/text()")
                if len(temp) > 0:
                    self.reviews = temp[0].strip()
                    self.reviews = str(self.reviews).split('+')[0]
                    self.reviews = str(self.reviews).split(' ')[0].strip()
                if self.reviews == "No":
                    self.reviews = 0
            else:
                # try old page match
                temp = tree.xpath(
                    "//span[@itemprop='reviewCount']/text()"
                    )
                if len(temp) > 0:
                    self.reviews = temp[0]
            if self.reviews is not None:
                self.reviews = int(self.reviews)
        except IndexError:
            return
        except Exception as e:
            logger.exception(e)
            self.reviews = None

    def __get_accommodates(self, tree):
        try:
            # 2016-04-10
            s = tree.xpath("//meta[@id='_bootstrap-listing']/@content")
            temp = tree.xpath(
                "//div[@class='col-md-6']"
                "/div/span[text()[contains(.,'Accommodates:')]]"
                "/../strong/text()"
                )
            if s is not None:
                j = json.loads(s[0])
                self.accommodates = j["listing"]["person_capacity"]
                return
            elif len(temp) > 0:
                self.accommodates = temp[0].strip()
            else:
                temp = tree.xpath(
                    "//div[@class='col-md-6']"
                    "/div[text()[contains(.,'Accommodates:')]]"
                    "/strong/text()"
                    )
                if len(temp) > 0:
                    self.accommodates = temp[0].strip()
                else:
                    temp = tree.xpath(
                        "//div[@class='col-md-6']"
                        "//div[text()[contains(.,'Accommodates:')]]"
                        "/strong/text()"
                    )
                    if len(temp) > 0:
                        self.accommodates = temp[0].strip()
            if type(self.accommodates) == str:
                self.accommodates = self.accommodates.split('+')[0]
                self.accommodates = self.accommodates.split(' ')[0]
            self.accommodates = int(self.accommodates)
        except:
            self.accommodates = None

    def __get_bedrooms(self, tree):
        try:
            temp = tree.xpath(
                "//div[@class='col-md-6']"
                "/div/span[text()[contains(.,'Bedrooms:')]]"
                "/../strong/text()"
                )
            if len(temp) > 0:
                self.bedrooms = temp[0].strip()
            else:
                temp = tree.xpath(
                    "//div[@class='col-md-6']"
                    "/div[text()[contains(.,'Bedrooms:')]]"
                    "/strong/text()"
                    )
                if len(temp) > 0:
                    self.bedrooms = temp[0].strip()
            if self.bedrooms:
                self.bedrooms = self.bedrooms.split('+')[0]
                self.bedrooms = self.bedrooms.split(' ')[0]
            self.bedrooms = float(self.bedrooms)
        except:
            self.bedrooms = None

    def __get_bathrooms(self, tree):
        try:
            temp = tree.xpath(
                "//div[@class='col-md-6']"
                "/div/span[text()[contains(.,'Bathrooms:')]]"
                "/../strong/text()"
                )
            if len(temp) > 0:
                self.bathrooms = temp[0].strip()
            else:
                temp = tree.xpath(
                    "//div[@class='col-md-6']"
                    "/div/span[text()[contains(.,'Bathrooms:')]]"
                    "/../strong/text()"
                    )
                if len(temp) > 0:
                    self.bathrooms = temp[0].strip()
            if self.bathrooms:
                self.bathrooms = self.bathrooms.split('+')[0]
                self.bathrooms = self.bathrooms.split(' ')[0]
            self.bathrooms = float(self.bathrooms)
        except:
            self.bathrooms = None

    def __get_minstay(self, tree):
        try:
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
                self.minstay = temp3[0].strip()
            elif len(temp2) > 0:
                self.minstay = temp2[0].strip()
            elif len(temp1) > 0:
                self.minstay = temp1[0].strip()
            if self.minstay is not None:
                self.minstay = self.minstay.split('+')[0]
                self.minstay = self.minstay.split(' ')[0]
            self.minstay = int(self.minstay)
        except:
            self.minstay = None

    def __get_price(self, tree):
        try:
            temp2 = tree.xpath(
                "//meta[@itemprop='price']/@content"
                )
            temp1 = tree.xpath(
                "//div[@id='price_amount']/text()"
                )
            if len(temp2) > 0:
                self.price = temp2[0]
            elif len(temp1) > 0:
                self.price = temp1[0][1:]
                non_decimal = re.compile(r'[^\d.]+')
                self.price = non_decimal.sub('', self.price)
            # Now find out if it's per night or per month
            # (see if the per_night div is hidden)
            per_month = tree.xpath(
                "//div[@class='js-per-night book-it__payment-period  hide']")
            if per_month:
                self.price = int(int(self.price) / 30)
            self.price = int(self.price)
        except:
            self.price = None

    def __get_room_info_from_tree(self, tree, flag):
        try:
            # Some of these items do not appear on every page (eg,
            # ratings, bathrooms), and so their absence is marked with
            # logger.info. Others should be present for every room (eg,
            # latitude, room_type, host_id) and so are marked with a
            # warning.  Items coded in <meta
            # property="airbedandbreakfast:*> elements -- country --

            self.__get_country(tree)
            self.__get_city(tree)
            self.__get_rating(tree)
            self.__get_latitude(tree)
            self.__get_longitude(tree)
            self.__get_host_id(tree)
            self.__get_room_type(tree)
            self.__get_neighborhood(tree)
            self.__get_address(tree)
            self.__get_reviews(tree)
            self.__get_accommodates(tree)
            self.__get_bedrooms(tree)
            self.__get_bathrooms(tree)
            self.__get_minstay(tree)
            self.__get_price(tree)
            self.deleted = 0

            # NOT FILLING HERE, but maybe should? have to write helper methods:
            # coworker_hosted, extra_host_languages, name,
            #    property_type, currency, rate_type

            self.status_check()

            if flag == FLAGS_ADD:
                self.save(FLAGS_INSERT_REPLACE)
            elif flag == FLAGS_PRINT:
                self.print_from_web_site()
            return True
        except KeyboardInterrupt:
            raise
        except IndexError:
            logger.exception("Web page has unexpected structure.")
            raise
        except UnicodeEncodeError as uee:
            logger.exception("UnicodeEncodeError Exception at " +
                             str(uee.object[uee.start:uee.end]))
            raise
        except AttributeError:
            logger.exception("AttributeError")
            raise
        except TypeError:
            logger.exception("TypeError parsing web page.")
            raise
        except Exception:
            logger.exception("Error parsing web page.")
            raise

# ==============================================================================
# End of class Listing
# ==============================================================================

# ==============================================================================
# Survey class: information and methods around a search
# ==============================================================================


class Survey():

    def __init__(self, config, survey_id):
        self.config = config
        self.survey_id = survey_id
        self.search_area_id = None
        self.search_area_name = None
        self.__set_search_area()

    def __global_search(self):
        """
        Special search to randomly choose rooms from a range rather than to
        look at specific areas of the world.
        """
        room_count = 0
        while room_count < self.config.FILL_MAX_ROOM_COUNT:
            try:
                # get a random candidate room_id
                room_id = random.randint(0, self.config.ROOM_ID_UPPER_BOUND)
                listing = Listing(self.config, room_id, self.survey_id)
                if room_id is None:
                    break
                else:
                    if listing.ws_get_room_info(FLAGS_ADD):
                        room_count += 1
            except AttributeError:
                logger.error(
                    "Attribute error: marking room as deleted.")
                listing.save_as_deleted()
            except Exception as ex:
                logger.exception("Error in search:" + str(type(ex)))
                raise

    def search(self, flag, search_by):
        logger.info("-" * 70)
        logger.info("Survey {survey_id}, for {search_area_name}".format(
            survey_id=self.survey_id, search_area_name=self.search_area_name
        ))
        self.__update_survey_entry(search_by)
        if self.search_area_name == SEARCH_AREA_GLOBAL:
            # "Special case": global search
            self.__global_search()
        else:
            if search_by == SEARCH_BY_BOUNDING_BOX:
                logger.info("Searching by bounding box")
                self.__search_loop_bounding_box(flag)
                # logger.info("Searching by bounding box - logged")
                # self.__search_loop_bounding_box_logged(flag)
                pass
            elif search_by == SEARCH_BY_ZIPCODE:
                logger.info("Searching by zipcode")
                zipcodes = db_get_zipcodes_from_search_area(self.config,
                                                            self.search_area_id)
                for room_type in (
                        "Private room",
                        "Entire home/apt",
                        "Shared room",):
                    self.__search_loop_zipcodes(zipcodes, room_type, flag)
            else:
                logger.info("Searching by neighborhood")
                neighborhoods = db_get_neighborhoods_from_search_area(
                    self.config, self.search_area_id)
                # for some cities (eg Havana) the neighbourhood information
                # is incomplete, and an additional search with no
                # neighbourhood is useful
                neighborhoods = neighborhoods + [None]
                for room_type in ("Private room",
                                  "Entire home/apt", "Shared room",):
                    logger.debug(
                        "Searching for %(rt)s by neighborhood",
                        {"rt": room_type})
                    if len(neighborhoods) > 0:
                        self.__search_loop_neighborhoods(neighborhoods,
                                                         room_type, flag)
                    else:
                        self.__search_neighborhood(None, room_type, flag)

    def log_progress(self, room_type, neighborhood_id,
                     guests, page_number, has_rooms):
        """ Add an entry to the survey_progress_log table to record the fact
        that a page has been visited.
        """
        try:
            page_info = (self.survey_id, room_type, neighborhood_id,
                         guests, page_number, has_rooms)
            logger.debug("Survey search page: " + str(page_info))
            sql = """
            insert into survey_progress_log
            (survey_id, room_type, neighborhood_id,
            guests, page_number, has_rooms)
            values (%s, %s, %s, %s, %s, %s)
            """
            conn = self.config.connect()
            cur = conn.cursor()
            cur.execute(sql, page_info)
            cur.close()
            conn.commit()
            logger.debug("Logging survey search page for neighborhood " +
                         str(neighborhood_id))
            return True
        except psycopg2.Error as pge:
            logger.error(pge.pgerror)
            cur.close()
            conn.rollback()
            return False
        except Exception:
            logger.error("Save survey search page failed")
            return False

    def log_progress_bounding_box(self, room_type, guests, zoomstack, page_number, has_rooms):
        """ Update survey_progress_log table to record current zoom stack and page number .
        """
        try:
            zoomstack_str = ''
            for zs in zoomstack:
                zoomstack_str = zoomstack_str + str(zs)
            page_info = (self.survey_id, room_type, guests, zoomstack_str, page_number, has_rooms)
            logger.debug("Survey search progress  - bounding box: " + str(page_info))
            sql_insert = """
            insert into survey_progress_log
            (survey_id, room_type,
            guests, zoomstack, page_number, has_rooms)
            values (%s, %s, %s, %s, %s, %s)
            """
            sql_delete = """
            delete from survey_progress_log where survey_id = %s
            """
            conn = self.config.connect()
            cur = conn.cursor()
            # logger.debug("sql_delete string: %s", sql_delete)
            cur.execute(sql_delete, (self.survey_id,))
            # logger.debug("sql_insert string: %s", sql_insert)
            cur.execute(sql_insert, page_info)
            cur.close()
            conn.commit()
            # logger.debug("Logging survey search page for neighborhood " + str(neighborhood_id))
            return True
        except psycopg2.Error as pge:
            logger.error(pge.pgerror)
            cur.close()
            conn.rollback()
            return False
        except Exception:
            logger.error("Save survey search page failed")
            return False

    def __update_survey_entry(self, search_by):
        try:
            survey_info = (date.today(),
                           search_by,
                           self.survey_id, )
            sql = """
            update survey
            set survey_date = %s, survey_method = %s
            where survey_id = %s
            """
            conn = self.config.connect()
            cur = conn.cursor()
            cur.execute(sql, survey_info)
            return True
        except psycopg2.Error as pge:
            logger.error(pge.pgerror)
            cur.close()
            conn.rollback()
            return False

    def __set_search_area(self):
        try:
            conn = self.config.connect()
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
            logger.error("No search area for survey_id " + str(self.survey_id))
            raise

    def __search_loop_bounding_box(self, flag):
        """
        A bounding box is a rectangle around a city, specified in the
        search_area table. The loop goes to quadrants of the bounding box
        rectangle and, if new listings are found, breaks that rectangle
        into four quadrants and tries again, recursively.
        The rectangles, including the bounding box, are represented by
        (n_lat, e_lng, s_lat, w_lng).
        """
        try:
            conn = self.config.connect()
            cur = conn.cursor()
            cur.execute("""
                        select bb_n_lat, bb_e_lng, bb_s_lat, bb_w_lng
                        from search_area sa join survey s
                        on sa.search_area_id = s.search_area_id
                        where s.survey_id = %s""", (self.survey_id,))
            bounding_box = cur.fetchone()
            cur.close()

            # check bounding box
            if None in bounding_box:
                logger.error("Invalid bounding box: contains 'None'")
                return
            if bounding_box[0] <= bounding_box[2]:
                logger.error("Invalid bounding box: n_lat must be > s_lat")
                return
            if bounding_box[1] <= bounding_box[3]:
                logger.error("Invalid bounding box: e_lng must be > w_lng")
                return

            logger.info("Bounding box: " + str(bounding_box))
            price_increments = [0, 40, 60, 80, 100, 120,
                                140, 180, 200,
                                300, 500,
                                700, 1000, 1500, 10000]
            for room_type in ("Private room", "Entire home/apt", "Shared room"):
                if room_type in ("Private room", "Shared room"):
                    max_guests = 4
                else:
                    max_guests = self.config.SEARCH_MAX_GUESTS
                for guests in range(1, max_guests):
                    for i in range(len(price_increments) - 1):
                        price_range = [price_increments[i], price_increments[i+1]]
                        # TS: move this max_price thing out of the loop
                        max_price = {"Private room": 500,
                                     "Entire home/apt": 10000,
                                     "Shared room": 500}
                        rectangle_zoom = 0
                        if price_range[1] > max_price[room_type]:
                            continue
                        self.__search_rectangle(
                            room_type, guests, price_range, bounding_box,
                            rectangle_zoom, flag)
        except Exception:
            logger.exception("Error")

    def __search_loop_bounding_box_logged(self, flag):
        """
        A bounding box is a rectangle around a city, specified in the
        search_area table. The loop goes to quadrants of the bounding box
        rectangle and, if new listings are found, breaks that rectangle
        into four quadrants and tries again, recursively.
        The rectangles, including the bounding box, are represented by
        (n_lat, e_lng, s_lat, w_lng).
        """
        try:
            conn = self.config.connect()
            cur = conn.cursor()
            cur.execute("""
                        select bb_n_lat, bb_e_lng, bb_s_lat, bb_w_lng
                        from search_area sa join survey s
                        on sa.search_area_id = s.search_area_id
                        where s.survey_id = %s""", (self.survey_id,))
            bounding_box = cur.fetchone()
            cur.close()

            # check bounding box
            if None in bounding_box:
                logger.error("Invalid bounding box: contains 'None'")
                return
            if bounding_box[0] <= bounding_box[2]:
                logger.error("Invalid bounding box: n_lat must be > s_lat")
                return
            if bounding_box[1] <= bounding_box[3]:
                logger.error("Invalid bounding box: e_lng must be > w_lng")
                return

            logger.info("Bounding box: " + str(bounding_box))

            # check for resume information for partially completed surveys in logger
            try:
                cur = conn.cursor()
                cur.execute("""
                        select room_type, page_number, guests, zoomstack
                        from survey_progress_log
                        where survey_id = %s """, (self.survey_id,))
                (log_room_type, log_page_number, log_guests, log_zoomstack_str) = cur.fetchone()
                cur.close()
                log_zoomstack = []
                # turn zoomstack string into list
                for i in range(len(log_zoomstack_str)):
                    log_zoomstack.append(int(log_zoomstack_str[i]))
                log_resuming = True
                logger.info("Resuming bounding_box search: ")
                logger.info("    room_type: %s, page_nunber: %d, guests: %d, zoomstack: %s",
                            log_room_type, log_page_number, log_guests, str(log_zoomstack))
            except:
                cur.close()
                logger.info("No resume information available")
                # set up defaults here
                log_zoomstack = []
                log_page_number = 1
                log_room_type = "Private room"
                log_guests = 1
                log_resuming = False

            # skip outer loops according to log state and propagate zoomstack and page number
            # inward on logged room_type
            # NB: THERE SEEMS TO BE NO POINT IN ITERATING OVER GUEST NUMBERS -
            # 1 GUEST SHOULD RETURN EVERYTHING - SO STRIPPED LOOP TO ROOM TYPES ONLY
            guests = 1
            rectangle_zoom = 0
            for room_type in ("Private room", "Entire home/apt", "Shared room"):
                # skip outer loop if resuming until get to logged room_type
                if log_resuming and not (room_type == log_room_type):
                    logger.info("skipping room type %s", room_type)
                    continue
                # if resuming and at logged room_type, search and propagate zoomstack and page number from logs inward
                elif log_resuming and room_type == log_room_type:
                    log_resuming = False
                    self.__search_rectangle_logged(room_type, guests, bounding_box,
                                                   rectangle_zoom, flag, [], log_zoomstack, log_page_number)
                # if not resuming or have already done resume loop, continue with no resume info
                else:
                    self.__search_rectangle_logged(room_type, guests, bounding_box, rectangle_zoom, flag, [], [], 1)
        except Exception:
            logger.exception("Error")

    def __search_rectangle(self, room_type, guests, price_range, rectangle,
                           rectangle_zoom, flag):
        """
        Recursive function to search for listings inside a rectangle.
        The actual search calls are done in ws_search_rectangle, and
        this method prints output and sets up new rectangles, if necessary,
        for another round of searching.
        """
        try:
            (new_rooms, page_number) = ws_search_rectangle(self, room_type, guests, price_range,
                                                           rectangle, rectangle_zoom, flag)
            logger.info(("{room_type} ({g} guests): zoom {rect_zoom}: "
                         "{new_rooms} new rooms, {page_number} pages").format(
                             room_type=room_type, g=str(guests),
                             rect_zoom=str(rectangle_zoom),
                             new_rooms=str(new_rooms),
                             page_number=str(page_number)))
            # The max zoom is set in config, but decrease it by one for each guest
            # so that high guest counts don't zoom in (which turns out to generate
            # very few new rooms but take a lot of time)
            if rectangle_zoom < max(1, (self.config.SEARCH_MAX_RECTANGLE_ZOOM - 2 *(guests - 1))):
                zoomable = True
            else:
                zoomable = False
            # TS: temporary experiment
            # if (new_rooms > 0 or page_number == self.config.SEARCH_MAX_PAGES) and zoomable:
            # zoom in if there are new rooms, or (to deal with occasional cases) if
            # the search returned a full set of SEARCH_MAX_PAGES pages even if no rooms
            # were new.
            if page_number == self.config.SEARCH_MAX_PAGES and zoomable:
                # break the rectangle into quadrants
                # (n_lat, e_lng, s_lat, w_lng).
                (n_lat, e_lng, s_lat, w_lng) = rectangle
                mid_lat = (n_lat + s_lat)/2.0
                mid_lng = (e_lng + w_lng)/2.0
                rectangle_zoom += 1
                # overlap quadrants to ensure coverage at high zoom levels
                # Airbnb max zoom (18) is about 0.004 on a side.
                blur = abs(n_lat - s_lat) * SEARCH_RECTANGLE_EDGE_BLUR
                logger.debug("-> mid_lat={midlat:+.5f}, midlng={midlng:+.5f}, blur = {blur:+.5f}".
                             format(blur=blur, midlat=mid_lat, midlng=mid_lng))
                quadrant = (n_lat + blur, e_lng + blur,
                            mid_lat - blur, mid_lng - blur)
                new_rooms = self.__search_rectangle(room_type, guests, price_range,
                                                    quadrant, rectangle_zoom, flag)
                quadrant = (n_lat + blur, mid_lng + blur,
                            mid_lat - blur, w_lng - blur)
                new_rooms = self.__search_rectangle(room_type, guests, price_range,
                                                    quadrant, rectangle_zoom, flag)
                quadrant = (mid_lat + blur, e_lng + blur,
                            s_lat - blur, mid_lng - blur)
                new_rooms = self.__search_rectangle(room_type, guests, price_range,
                                                    quadrant, rectangle_zoom, flag)
                quadrant = (mid_lat + blur, mid_lng + blur,
                            s_lat - blur, w_lng - blur)
                new_rooms = self.__search_rectangle(room_type, guests, price_range,
                                                    quadrant, rectangle_zoom, flag)
            if flag == FLAGS_PRINT:
                # for FLAGS_PRINT, fetch one page and print it
                sys.exit(0)
        except TypeError as te:
            logger.error("TypeError in __search_rectangle")
            logger.error(te.args)
            raise
        except:
            logger.error("Error in __search_rectangle")
            raise

    def __search_loop_zipcodes(self, zipcodes, room_type, flag):
        try:
            i = 0
            for zipcode in zipcodes:
                i += 1
                self.__search_zipcode(str(zipcode), room_type, self.survey_id,
                                      flag, self.search_area_name)
        except Exception:
            raise

    def __search_zipcode(self, zipcode, room_type, survey_id,
                         flag, search_area_name):
        try:
            if room_type in ("Private room", "Shared room"):
                max_guests = 4
            else:
                max_guests = self.config.SEARCH_MAX_GUESTS
            for guests in range(1, max_guests):
                logger.info("Searching for %(g)i guests", {"g": guests})
                for page_number in range(1, self.config.SEARCH_MAX_PAGES + 1):
                    if flag != FLAGS_PRINT:
                        # this efficiency check can be implemented later
                        count = page_has_been_retrieved(
                            self.config, survey_id, room_type, str(zipcode),
                            guests, page_number, SEARCH_BY_ZIPCODE)
                        if count == 1:
                            logger.info(
                                "\t...search page has been visited previously")
                            continue
                        elif count == 0:
                            logger.info(
                                "\t...search page has been visited previously")
                            break
                        else:
                            logger.debug("\t...visiting search page")
                    room_count = ws_get_search_page_info_zipcode(
                        self, room_type, zipcode, guests, page_number, flag)
                    if flag == FLAGS_PRINT:
                        # for FLAGS_PRINT, fetch one page and print it
                        sys.exit(0)
                    if room_count < SEARCH_LISTINGS_ON_FULL_PAGE:
                        logger.debug("Final page of listings for this search")
                        break
        except Exception:
            raise

    def __search_rectangle_logged(self, room_type, guests, rectangle,
                                  rectangle_zoom, flag, zoomstack, zoomresume, pageresume):
        # zoomresume is a list of zoom quadrants to skip to when resuming a search from a log
        # zoomstack is the list of zoom quadrants up the call stack, to be saved for resuming

        logger.debug("search_rectangle_logged rectangle: %s, zoom: %d, zoomstack: %s, zoomresume: %s, pageresume: %d",
                     str(rectangle), rectangle_zoom, str(zoomstack), str(zoomresume), pageresume)

        zoomresuming = (len(zoomresume) > 0)

        new_rooms = 0
        room_total = 0
        page_number = 1

        if (not zoomresuming):
            # if  rectangle_zoom == SEARCH_MAX_RECTANGLE_ZOOM:
            # # THIS CALLS A LOGGED VERSION TO RECORD ZOOMSTACK, guests, room_type
            # # IN A DB TABLE to resume from interruptions
            # # have edited ws_search_rectangle_logged to return rooms not new rooms;
            # I believe looking only for new rooms can lead to omissions
            (new_rooms, room_total, page_number) = ws_search_rectangle_logged(self, room_type, guests,
                                                                              rectangle, rectangle_zoom,
                                                                              flag, zoomstack, pageresume)
            logger.info(("{room_type} ({g} guests): zoom {rect_zoom}: "
                         "{new_rooms} new rooms.").format(room_type=room_type, g=str(guests),
                                                          rect_zoom=str(rectangle_zoom),
                                                          new_rooms=str(new_rooms)))

        zoomable = True if rectangle_zoom < self.config.SEARCH_MAX_RECTANGLE_ZOOM else False
        if (zoomresuming or (room_total > 0 and zoomable and page_number > self.config.SEARCH_MAX_PAGES)):
            # elif rectangle_zoom < SEARCH_MAX_RECTANGLE_ZOOM:
            # break the rectangle into quadrants
            # (n_lat, e_lng, s_lat, w_lng).
            (n_lat, e_lng, s_lat, w_lng) = rectangle
            mid_lat = (n_lat + s_lat)/2.0
            mid_lng = (e_lng + w_lng)/2.0
            rectangle_zoom += 1
            # overlap quadrants to ensure coverage at high zoom levels
            # Airbnb max zoom (18) is about 0.004 on a side.
            blur = abs(n_lat - s_lat) * SEARCH_RECTANGLE_EDGE_BLUR
            i = 1
            newzoomresume = []
            pr = 1
            if zoomresuming:
                # if resuming, skip to appropriate zoom quadrant;
                i = zoomresume[0]
                # new resume list is tail of current one
                newzoomresume = zoomresume[1:]
                pr = pageresume
            while i < 5:
                if i == 1:
                    quadrant = (n_lat + blur, e_lng - blur, mid_lat - blur, mid_lng + blur)
                    # SK TOCHECK: QUADRANT COORDS for all of these
                    logger.debug("NE Quadrant: %s", str(quadrant))
                    logger.debug("Quadrant size: {lat} by {lng}".format(
                                                             lat=str(quadrant[0] - quadrant[2]),
                                                             lng=str(abs(quadrant[1] - quadrant[3]))))

                    newzoomstack = zoomstack + [1]
                    # logger.debug("zs: %s, newzs: %s", str(zoomstack), str(newzoomstack))
                    self.__search_rectangle_logged(room_type, guests,
                                                   quadrant, rectangle_zoom, flag,
                                                   newzoomstack, newzoomresume, pr)
                elif i == 2:
                    quadrant = (n_lat + blur, mid_lng - blur, mid_lat - blur, w_lng + blur)
                    logger.debug("NW Quadrant: %s", str(quadrant))
                    logger.debug("Quadrant size: {lat} by {lng}".format(
                                                                         lat=str(quadrant[0] - quadrant[2]),
                                                                         lng=str(abs(quadrant[1] - quadrant[3]))))
                    newzoomstack = zoomstack + [2]
                    self.__search_rectangle_logged(room_type, guests,
                                                   quadrant, rectangle_zoom, flag, newzoomstack, newzoomresume, pr)

                elif i == 3:
                    quadrant = (mid_lat + blur, e_lng - blur, s_lat - blur, mid_lng + blur)

                    logger.debug("SE Quadrant: %s", str(quadrant))
                    logger.debug("Quadrant size: {lat} by {lng}".format(
                                                                         lat=str(quadrant[0] - quadrant[2]),
                                                                         lng=str(abs(quadrant[1] - quadrant[3]))))

                    newzoomstack = zoomstack + [3]
                    self.__search_rectangle_logged(room_type, guests,
                                                   quadrant, rectangle_zoom, flag, newzoomstack, newzoomresume, pr)

                elif i == 4:
                    quadrant = (mid_lat + blur, mid_lng - blur, s_lat - blur, w_lng + blur)
                    logger.debug("SW Quadrant: %s", str(quadrant))
                    logger.debug("Quadrant size: {lat} by {lng}".format(
                                                                         lat=str(quadrant[0] - quadrant[2]),
                                                                         lng=str(abs(quadrant[1] - quadrant[3]))))
                    newzoomstack = zoomstack + [4]
                    self.__search_rectangle_logged(room_type, guests,
                                                   quadrant, rectangle_zoom, flag, newzoomstack, newzoomresume, pr)
                # no matter what, we will only be resuming on first iteration of loop
                newzoomresume = []
                pr = 1
                i += 1

        if flag == FLAGS_PRINT:
            # for FLAGS_PRINT, fetch one page and print it
            sys.exit(0)

    def __search_loop_neighborhoods(self, neighborhoods, room_type, flag):
        """Loop over neighborhoods in a city. No return."""
        try:
            for neighborhood in neighborhoods:
                self.__search_neighborhood(neighborhood, room_type, flag)
        except Exception:
            raise

    def __search_neighborhood(self, neighborhood, room_type, flag):
        try:
            if room_type in ("Private room", "Shared room"):
                max_guests = 4
            else:
                max_guests = self.config.SEARCH_MAX_GUESTS
            for guests in range(1, max_guests):
                logger.debug("Searching for %(g)i guests", {"g": guests})
                for page_number in range(1, self.config.SEARCH_MAX_PAGES + 1):
                    if flag != FLAGS_PRINT:
                        count = page_has_been_retrieved(
                            self.config, self.survey_id, room_type,
                            neighborhood, guests, page_number,
                            SEARCH_BY_NEIGHBORHOOD)
                        if count == 1:
                            logger.info(
                                "\t...search page has been visited previously")
                            continue
                        elif count == 0:
                            logger.info(
                                "\t...search page has been visited previously")
                            break
                        else:
                            pass
                    room_count = self.__search_neighborhood_page(
                        room_type, neighborhood, guests, page_number, flag)
                    logger.info(("{room_type} ({g} guests): neighbourhood {neighborhood}: "
                         "{room_count} rooms, {page_number} pages").format(
                             room_type=room_type, g=str(guests),
                             neighborhood = neighborhood,
                             room_count = room_count,
                             page_number=str(page_number)))

                    if flag == FLAGS_PRINT:
                        # for FLAGS_PRINT, fetch one page and print it
                        sys.exit(0)
                    if room_count < SEARCH_LISTINGS_ON_FULL_PAGE:
                        logger.debug("Final page of listings for this search")
                        break
        except Exception:
            raise

    def __search_neighborhood_page(self, room_type, neighborhood, guests, page_number, flag):
        try:
            logger.info("-" * 70)
            logger.info(
                "Survey " + str(self.survey_id) + " (" +
                self.search_area_name + "): " +
                room_type + ", " +
                str(neighborhood) + ", " +
                str(guests) + " guests, " +
                "page " + str(page_number))
            new_rooms = 0
            room_count = 0
            params = {}
            params["page"] = str(page_number)
            params["source"] = "filter"
            params["location"] = self.search_area_name
            params["room_types[]"] = room_type
            params["neighborhoods[]"] = neighborhood
            response = ws_request_with_repeats(self.config, URL_API_SEARCH_ROOT, params)
            json = response.json()
            for result in json["results_json"]["search_results"]:
                room_id = int(result["listing"]["id"])
                if room_id is not None:
                    room_count += 1
                    listing = self.listing_from_search_page_json(result, self, room_id, room_type)
                    if listing is None:
                        continue
                    if listing.host_id is not None:
                        listing.deleted = 0
                        if flag == FLAGS_ADD:
                            if listing.save(FLAGS_INSERT_NO_REPLACE):
                                new_rooms += 1
                        elif flag == FLAGS_PRINT:
                            print(room_type, listing.room_id)
            if room_count > 0:
                has_rooms = 1
            else:
                has_rooms = 0
            if flag == FLAGS_ADD:
                neighborhood_id = db_get_neighborhood_id(self.config,
                                                         self.survey_id, neighborhood)
                self.log_progress(room_type, neighborhood_id,
                                  guests, page_number, has_rooms)
            return room_count
        except UnicodeEncodeError:
            logger.error("UnicodeEncodeError: set PYTHONIOENCODING=utf-8")
            # if sys.version_info >= (3,):
            #    logger.info(s.encode('utf8').decode(sys.stdout.encoding))
            # else:
            #    logger.info(s.encode('utf8'))
            # unhandled at the moment
        except Exception:
            raise

    def listing_from_search_page_json(self, result, survey, room_id, room_type):
        try:
            listing = Listing(self.config, room_id, survey.survey_id, room_type)
            # listing information
            json_listing = result["listing"]
            listing.host_id = json_listing["primary_host"]["id"] if "primary_host" in json_listing else None
            listing.address = json_listing["public_address"] if "public_address" in json_listing else None
            listing.reviews = json_listing["reviews_count"] if "reviews_count" in json_listing else None
            listing.overall_satisfaction = json_listing["star_rating"] if "star_rating" in json_listing else None
            listing.accommodates = json_listing["person_capacity"] if "person_capacity" in json_listing else None
            listing.bedrooms = json_listing["bedrooms"] if "bedrooms" in json_listing else None
            listing.latitude = json_listing["lat"] if "lat" in json_listing else None
            listing.longitude = json_listing["lng"] if "lng" in json_listing else None
            listing.coworker_hosted = json_listing["coworker_hosted"] if "coworker_hosted" in json_listing else None
            listing.extra_host_languages = json_listing["extra_host_languages"] \
                if "extra_host_languages" in json_listing else None
            listing.name = json_listing["name"] if "name" in json_listing else None
            listing.property_type = json_listing["property_type"] if "property_type" in json_listing else None
            # pricing
            json_pricing = result["pricing_quote"]
            listing.price = json_pricing["rate"]["amount"] if "rate" in json_pricing else None
            listing.currency = json_pricing["rate"]["currency"] if "rate" in json_pricing else None
            listing.rate_type = json_pricing["rate_type"] if "rate_type" in json_pricing else None
            return listing
        except:
            logger.exception("Error in survey.listing_from_search_page_json: returning None")
            sys.exit(-1)
            return None


# ==============================================================================
# End of class Survey
# ==============================================================================


def list_search_area_info(config, search_area):
    try:
        conn = config.connect()
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


def list_surveys(config):
    try:
        conn = config.connect()
        cur = conn.cursor()
        cur.execute("""
            select survey_id, to_char(survey_date, 'YYYY-Mon-DD'),
                    survey_description, search_area_id
            from survey
            order by survey_id asc""")
        result_set = cur.fetchall()
        if len(result_set) > 0:
            template = "| {0:3} | {1:>12} | {2:>30} | {3:3} |"
            print(template.format("ID", "Date", "Description", "SA"))
            for survey in result_set:
                (survey_id, survey_date, desc, sa_id) = survey
                print(template.format(survey_id, survey_date, desc, sa_id))
    except Exception:
        logger.error("Cannot list surveys.")
        raise


def db_ping(config):
    try:
        conn = config.connect()
        if conn is not None:
            print("Connection test succeeded")
        else:
            print("Connection test failed")
    except Exception:
        logger.exception("Connection test failed")


def db_add_survey(config, search_area):
    try:
        conn = config.connect()
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
        print("\nSurvey added:\n" +
              "\n\tsurvey_id=" + str(survey_id) +
              "\n\tsurvey_date=" + str(survey_date) +
              "\n\tsurvey_description=" + survey_description +
              "\n\tsearch_area_id=" + str(search_area_id))
    except Exception:
        logger.error("Failed to add survey for " + search_area)
        raise


def db_get_zipcodes_from_search_area(config, search_area_id):
    try:
        conn = config.connect()
        cur = conn.cursor()
        # Query from the manually-prepared zipcode table
        cur.execute("""
        select zipcode
        from zipcode z, search_area sa
        where sa.search_area_id = %s
        and z.search_area_id = sa.search_area_id
        """, (search_area_id,))
        zipcodes = []
        while True:
            row = cur.fetchone()
            if row is None:
                break
            zipcodes.append(row[0])
        cur.close()
        return zipcodes
    except Exception:
        logger.error("Failed to retrieve zipcodes for search_area" +
                     str(search_area_id))
        raise


def db_get_neighborhoods_from_search_area(config, search_area_id):
    try:
        conn = config.connect()
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
        logger.error("Failed to retrieve neighborhoods from " +
                     str(search_area_id))
        raise


def db_get_room_to_fill(config, survey_id):
    for attempt in range(config.MAX_CONNECTION_ATTEMPTS):
        try:
            conn = config.connect()
            cur = conn.cursor()
            if survey_id == 0:  # no survey specified
                sql = """
                    select room_id, survey_id
                    from room
                    where deleted is null
                    order by random()
                    limit 1
                    """
                cur.execute(sql)
            else:
                sql = """
                    select room_id, survey_id
                    from room
                    where deleted is null
                    and survey_id = %s
                    order by random()
                    limit 1
                    """
                cur.execute(sql, (survey_id,))
            (room_id, survey_id) = cur.fetchone()
            listing = Listing(config, room_id, survey_id)
            cur.close()
            conn.commit()
            return listing
        except TypeError:
            logger.info("Finishing: no unfilled rooms in database --")
            conn.rollback()
            del (config.connection)
            return None
        except Exception:
            logger.exception("Error retrieving room to fill from db")
            conn.rollback()
            del (config.connection)
    return None


def db_get_neighborhood_id(config, survey_id, neighborhood):
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
        conn = config.connect()
        cur = conn.cursor()
        cur.execute(sql, (survey_id, neighborhood,))
        neighborhood_id = cur.fetchone()[0]
        cur.close()
        conn.commit()
        return neighborhood_id
    except psycopg2.Error:
        raise
    except Exception:
        return None


def ws_get_city_info(config, city, flag):
    try:
        url = URL_SEARCH_ROOT + city
        response = ws_request_with_repeats(config, url)
        if response is None:
            return False
        tree = html.fromstring(response.text)
        try:
            citylist = tree.xpath(
                "//input[@name='location']/@value")
            neighborhoods = tree.xpath(
                "//input[contains(@id, 'filter-option-neighborhoods')]/@value")
            if flag == FLAGS_PRINT:
                print("\n", citylist[0])
                print("Neighborhoods:")
                for neighborhood in neighborhoods:
                    print("\t", neighborhood)
            elif flag == FLAGS_ADD:
                if len(citylist) > 0:
                    conn = config.connect()
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
                    # city_id = cur.lastrowid
                    sql_identity = """select
                    currval('search_area_search_area_id_seq')
                    """
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
                        insert into neighborhood(name, search_area_id)
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
            # if sys.version_info >= (3,):
            #    logger.info(s.encode('utf8').decode(sys.stdout.encoding))
            # else:
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
        response = requests.get(URL_ROOT)
        if response.status_code == requests.codes.ok:
            return True         # URL Exist
        else:
            return False
    except Exception:
        logger.exception("Exception in ws_airbnb_is_live")
        return False        # URL don't seem to be alive


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


def ws_search_rectangle(survey, room_type, guests, price_range,
                        rectangle, rectangle_zoom, flag):
    """
        rectangle is (n_lat, e_lng, s_lat, w_lng)
        returns number of *new* rooms and number of pages tested
    """
    try:
        logger.info("-" * 70)
        logger.info(("Searching '{room_type}' ({guests} guests, prices in [{p1}, {p2}]), "
                     "zoom {zoom}").format(room_type=room_type,
                                           guests=str(guests),
                                           p1=str(price_range[0]),
                                           p2=str(price_range[1]),
                                           zoom=str(rectangle_zoom)))
        logger.debug("Rectangle: N={n:+.5f}, E={e:+.5f}, S={s:+.5f}, W={w:+.5f}".format(
            n=rectangle[0], e=rectangle[1], s=rectangle[2], w=rectangle[3])
        )
        new_rooms = 0
        room_total = 0
        for page_number in range(1, survey.config.SEARCH_MAX_PAGES + 1):
            room_count = 0
            logger.info("Page " + str(page_number) + "...")
            params = {}
            params["guests"] = str(guests)
            params["page"] = str(page_number)
            params["source"] = "filter"
            params["room_types[]"] = room_type
            params["sw_lat"] = str(rectangle[2])
            params["sw_lng"] = str(rectangle[3])
            params["ne_lat"] = str(rectangle[0])
            params["ne_lng"] = str(rectangle[1])
            params["search_by_map"] = str(True)
            params["price_min"] = str(price_range[0])
            params["price_max"] = str(price_range[1])
            response = ws_request_with_repeats(survey.config, URL_API_SEARCH_ROOT, params)
            json = response.json()
            for result in json["results_json"]["search_results"]:
                room_id = int(result["listing"]["id"])
                if room_id is not None:
                    room_count += 1
                    room_total += 1
                    listing = survey.listing_from_search_page_json(result, survey, room_id, room_type)
                    if listing is None:
                        continue
                    if listing.host_id is not None:
                        listing.deleted = 0
                        if flag == FLAGS_ADD:
                            if listing.save(FLAGS_INSERT_NO_REPLACE):
                                new_rooms += 1
                        elif flag == FLAGS_PRINT:
                            print(room_type, listing.room_id)

            if flag == FLAGS_PRINT:
                # for FLAGS_PRINT, fetch one page and print it
                sys.exit(0)
            if room_count < SEARCH_LISTINGS_ON_FULL_PAGE:
                logger.debug("Final page of listings for this search")
                break
        return (new_rooms, page_number)
    except UnicodeEncodeError:
        logger.error("UnicodeEncodeError: set PYTHONIOENCODING=utf-8")
        # if sys.version_info >= (3,):
        #    logger.info(s.encode('utf8').decode(sys.stdout.encoding))
        # else:
        #    logger.info(s.encode('utf8'))
        # unhandled at the moment
    except Exception:
        logger.exception("Exception in get_search_page_info_rectangle")
        raise


def ws_search_rectangle_logged(survey, room_type, guests,
                               rectangle, rectangle_zoom, flag, zoomstack, resumepage):
    """
        rectangle is (n_lat, e_lng, s_lat, w_lng)
        returns number of *new* rooms
    """

    try:
        logger.info("-" * 70)
        logger.info(("Searching '{room_type}' ({guests} guests), "
                     "zoom {zoom}").format(room_type=room_type,
                                                 guests=str(guests),
                                                 zoom=str(rectangle_zoom)))
        new_rooms = 0
        room_total = 0
        if rectangle_zoom >= (survey.config.SEARCH_MAX_RECTANGLE_ZOOM - 1):
            truncate_criterion = 999999
        else:
            truncate_criterion = SEARCH_RECTANGLE_TRUNCATE_CRITERION

        # jump to resumepage - for resume after logger (otherwise is 1)
        page_number = resumepage
        if page_number > 1:
            logger.info("jumping to page %d", page_number)

        while page_number <= survey.config.SEARCH_MAX_PAGES:
            room_count = 0
            logger.info("Page " + str(page_number) + "...")
            params = {}
            params["guests"] = str(guests)
            params["page"] = str(page_number)
            params["source"] = "filter"
            params["room_types[]"] = room_type
            params["sw_lat"] = str(rectangle[2])
            params["sw_lng"] = str(rectangle[3])
            params["ne_lat"] = str(rectangle[0])
            params["ne_lng"] = str(rectangle[1])
            # testing -- SK added to force airbnb to only return results within rectangle
            params["search_by_map"] = str(True)
            response = ws_request_with_repeats(survey.config, URL_API_SEARCH_ROOT, params)
            # Airbnb update 2016-11-05: some responses contain no property_ids
            # key: pick up the room_id from elsewhere
            # (Could get more info)
            json = response.json()
            for result in json["results_json"]["search_results"]:
                json_listing = result["listing"]
                json_pricing = result["pricing_quote"]
                # logger.info("Found " + str(room_count) + " rooms")
                room_id = int(json_listing["id"])
                if room_id is not None:
                    room_count += 1
                    room_total += 1
                    listing = Listing(room_id, survey.survey_id, room_type)
                    # add all info available in json
                    # some not here -- bathroom, city, country, minstay, neighbourhood --
                    # since I haven't seen them in the json.
                    # maybe just not reported in my searches? add later?
                    # TBD: Error handling for missing json items?
                    listing.host_id = json_listing["primary_host"]["id"]
                    listing.address = json_listing["public_address"]
                    listing.reviews = json_listing["reviews_count"]
                    listing.overall_satisfaction = json_listing["star_rating"]
                    listing.accommodates = json_listing["person_capacity"]
                    listing.bedrooms = json_listing["bedrooms"]
                    listing.price = json_pricing["rate"]["amount"]
                    listing.latitude = json_listing["lat"]
                    listing.longitude = json_listing["lng"]
                    # test that listing is in rectangle
                    if(listing.latitude > rectangle[0] or
                       listing.latitude < rectangle[2] or
                       listing.longitude < rectangle[3] or
                       listing.longitude > rectangle[1]):
                        logger.info("Listing coords (%f,%f) outside of rect %s !!!",
                                    listing.latitude, listing.longitude, str(rectangle))

                    listing.coworker_hosted = json_listing["coworker_hosted"]
                    listing.extra_host_languages = json_listing["extra_host_languages"]
                    listing.name = json_listing["name"]
                    listing.property_type = json_listing["property_type"]
                    listing.currency = json_pricing["rate"]["currency"]
                    listing.rate_type = json_pricing["rate_type"]
                    if listing.host_id is not None:
                        listing.deleted = 0
                    if flag == FLAGS_ADD:
                        if listing.save(FLAGS_INSERT_NO_REPLACE):
                            new_rooms += 1
                    elif flag == FLAGS_PRINT:
                        print(room_type, listing.room_id)
            if flag == FLAGS_PRINT:
                # for FLAGS_PRINT, fetch one page and print it
                sys.exit(0)
            page_number += 1
            # log progress on this survey in DB - survey id, room_type, zoomstack, page_number are the important ones
            survey.log_progress_bounding_box(room_type, guests, zoomstack, page_number, room_count)

            if room_count < SEARCH_LISTINGS_ON_FULL_PAGE:
                logger.debug("Final page of listings for this search")
                break

            if new_rooms >= truncate_criterion:
                logger.info(("Found {new_rooms} new rooms "
                             "Truncating search and zooming in").format(
                                                                    new_rooms=str(new_rooms)))
                break
            if page_number > survey.config.SEARCH_MAX_PAGES:
                logger.debug("Reached MAX_PAGES on this search!")

        logger.debug("Found %d new_rooms of %d room_total", new_rooms, room_total)
        return (new_rooms, room_total, page_number)
    except UnicodeEncodeError:
        logger.error("UnicodeEncodeError: set PYTHONIOENCODING=utf-8")
        # if sys.version_info >= (3,):
        #    logger.info(s.encode('utf8').decode(sys.stdout.encoding))
        # else:
        #    logger.info(s.encode('utf8'))
        # unhandled at the moment
    except Exception:
        logger.exception("Exception in get_search_page_info_rectangle")
        raise


def ws_get_search_page_info_zipcode(survey, room_type,
                                    zipcode, guests, page_number, flag):
    try:
        logger.info("-" * 70)
        logger.info(room_type + ", zipcode " + str(zipcode) + ", " +
                    str(guests) + " guests, " + "page " + str(page_number))
        room_count = 0
        new_rooms = 0
        params = {}
        params["guests"] = str(guests)
        params["page"] = str(page_number)
        params["source"] = "filter"
        params["location"] = zipcode
        params["room_types[]"] = room_type
        response = ws_request_with_repeats(survey.config, URL_API_SEARCH_ROOT, params)
        json = response.json()
        for result in json["results_json"]["search_results"]:
            room_id = int(result["listing"]["id"])
            if room_id is not None:
                room_count += 1
                listing = survey.listing_from_search_page_json(result, survey, room_id, room_type)
                if listing is None:
                    continue
                if listing.host_id is not None:
                    listing.deleted = 0
                    if flag == FLAGS_ADD:
                        if listing.save(FLAGS_INSERT_NO_REPLACE):
                            new_rooms += 1
                    elif flag == FLAGS_PRINT:
                        print(room_type, listing.room_id)
        if room_count > 0:
            has_rooms = 1
        else:
            has_rooms = 0
        if flag == FLAGS_ADD:
            survey.log_progress(room_type, zipcode,
                                guests, page_number, has_rooms)
        else:
            logger.info("No rooms found")
        return room_count
    except UnicodeEncodeError:
        logger.error(
            "UnicodeEncodeError: you may want to set PYTHONIOENCODING=utf-8")
        # if sys.version_info >= (3,):
        #    logger.info(s.encode('utf8').decode(sys.stdout.encoding))
        # else:
        #    logger.info(s.encode('utf8'))
        # unhandled at the moment
    except Exception as e:
        logger.error("Exception type: " + type(e).__name__)
        raise


def display_room(room_id):
    webbrowser.open(URL_ROOM_ROOT + str(room_id))


def display_host(host_id):
    webbrowser.open(URL_HOST_ROOT + str(host_id))


def fill_loop_by_room(config, survey_id):
    """
    Master routine for looping over rooms (after a search)
    to fill in the properties.
    """
    room_count = 0
    while room_count < config.FILL_MAX_ROOM_COUNT:
        try:
            if len(config.HTTP_PROXY_LIST) == 0:
                logger.info(
                    "No proxies left: re-initialize after {0} seconds".format(
                        config.RE_INIT_SLEEP_TIME))
                time.sleep(config.RE_INIT_SLEEP_TIME)  # be nice
                config = ABConfig()
            room_count += 1
            listing = db_get_room_to_fill(config, survey_id)
            if listing is None:
                return None
            else:
                if listing.ws_get_room_info(FLAGS_ADD):
                    pass
                else:  # Airbnb now seems to return nothing if a room has gone
                    listing.save_as_deleted()
        except AttributeError:
            logger.error("Attribute error: marking room as deleted.")
            listing.save_as_deleted()
        except Exception as e:
            logger.error("Error in fill_loop_by_room:" + str(type(e)))
            raise


def page_has_been_retrieved(config, survey_id, room_type, neighborhood_or_zipcode,
                            guests, page_number, search_by):
    """
    Returns 1 if the page has been retrieved previously and has rooms
    Returns 0 if the page has been retrieved previously and has no rooms
    Returns -1 if the page has not been retrieved previously
    """
    conn = config.connect()
    cur = conn.cursor()
    has_rooms = 0
    try:
        if search_by == SEARCH_BY_NEIGHBORHOOD:
            neighborhood = neighborhood_or_zipcode
            # TODO: Currently fails when there are no neighborhoods
            if neighborhood is None:
                has_rooms = -1
            else:
                params = (survey_id, room_type, neighborhood, guests,
                          page_number,)
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
                logger.debug("has_rooms = " + str(has_rooms) +
                             " for neighborhood " + neighborhood)
        else:  # SEARCH_BY_ZIPCODE
            zipcode = int(neighborhood_or_zipcode)
            params = (survey_id, room_type, zipcode, guests, page_number,)
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
            logger.debug("has_rooms = " + str(has_rooms) +
                         " for zipcode " + str(zipcode))
    except Exception:
        has_rooms = -1
        logger.debug("Page has not been retrieved previously")
    finally:
        cur.close()
        return has_rooms



def parse_args():
    """
    Read and parse command-line arguments
    """
    parser = argparse.ArgumentParser(
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
    group.add_argument('-dbp', '--dbping',
                       action='store_true', default=False,
                       help='Test the database connection')
    group.add_argument('-dh', '--displayhost',
                       metavar='host_id', type=int,
                       help='display web page for host_id in browser')
    group.add_argument('-dr', '--displayroom',
                       metavar='room_id', type=int,
                       help='display web page for room_id in browser')
    group.add_argument('-f', '--fill', nargs='?',
                       metavar='survey_id', type=int, const=0,
                       help='fill details for rooms collected with -s')
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
    group.add_argument('-psn', '--printsearch_by_neighborhood',
                       metavar='survey_id', type=int,
                       help="""print first page of search information
                       for survey from the Airbnb web site,
                       by neighborhood""")
    group.add_argument('-psz', '--printsearch_by_zipcode',
                       metavar='survey_id', type=int,
                       help="""print first page of search information
                       for survey from the Airbnb web site,
                       by zipcode""")
    group.add_argument('-psb', '--printsearch_by_bounding_box',
                       metavar='survey_id', type=int,
                       help="""print first page of search information
                       for survey from the Airbnb web site,
                       by bounding_box""")
    group.add_argument('-s', '--search',
                       metavar='survey_id', type=int,
                       help='search for rooms using survey survey_id')
    group.add_argument('-sn', '--search_by_neighborhood',
                       metavar='survey_id', type=int,
                       help='search for rooms using survey survey_id')
    group.add_argument('-sb', '--search_by_bounding_box',
                       metavar='survey_id', type=int,
                       help="""search for rooms using survey survey_id,
                       by bounding box
                       """)
    group.add_argument('-sz', '--search_by_zipcode',
                       metavar='survey_id', type=int,
                       help="""search for rooms using survey_id,
                       by zipcode""")
    group.add_argument('-v', '--version',
                       action='version',
                       version='%(prog)s, version ' +
                       str(SCRIPT_VERSION_NUMBER))
    group.add_argument('-?', action='help')

    args = parser.parse_args()
    return (parser, args)


def main():
    ab_config = ABConfig()
    (parser, args) = parse_args()
    try:
        if args.search:
            survey = Survey(ab_config, args.search)
            survey.search(FLAGS_ADD, SEARCH_BY_NEIGHBORHOOD)
        elif args.search_by_neighborhood:
            survey = Survey(ab_config, args.search_by_neighborhood)
            survey.search(FLAGS_ADD, SEARCH_BY_NEIGHBORHOOD)
        elif args.search_by_zipcode:
            survey = Survey(ab_config, args.search_by_zipcode)
            survey.search(FLAGS_ADD, SEARCH_BY_ZIPCODE)
        elif args.search_by_bounding_box:
            survey = Survey(ab_config, args.search_by_bounding_box)
            survey.search(FLAGS_ADD, SEARCH_BY_BOUNDING_BOX)
        elif args.fill is not None:
            fill_loop_by_room(ab_config, args.fill)
        elif args.addsearcharea:
            ws_get_city_info(ab_config, args.addsearcharea, FLAGS_ADD)
        elif args.addsurvey:
            db_add_survey(ab_config, args.addsurvey)
        elif args.dbping:
            db_ping(ab_config)
        elif args.displayhost:
            display_host(ab_config, args.displayhost)
        elif args.displayroom:
            display_room(ab_config, args.displayroom)
        elif args.listsearcharea:
            list_search_area_info(ab_config, args.listsearcharea)
        elif args.listroom:
            listing = Listing(ab_config, args.listroom, None)
            listing.print_from_db()
        elif args.listsurveys:
            list_surveys(ab_config)
        elif args.printsearcharea:
            ws_get_city_info(ab_config, args.printsearcharea, FLAGS_PRINT)
        elif args.printroom:
            listing = Listing(ab_config, args.printroom, None)
            listing.ws_get_room_info(FLAGS_PRINT)
        elif args.printsearch:
            survey = Survey(ab_config, args.printsearch)
            survey.search(FLAGS_PRINT, SEARCH_BY_NEIGHBORHOOD)
        elif args.printsearch_by_neighborhood:
            survey = Survey(ab_config, args.printsearch_by_neighborhood)
            survey.search(FLAGS_PRINT, SEARCH_BY_NEIGHBORHOOD)
        elif args.printsearch_by_bounding_box:
            survey = Survey(ab_config, args.printsearch_by_bounding_box)
            survey.search(FLAGS_PRINT, SEARCH_BY_BOUNDING_BOX)
        elif args.printsearch_by_zipcode:
            survey = Survey(ab_config, args.printsearch_by_zipcode)
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
