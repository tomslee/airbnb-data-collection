#!/usr/bin/python3
# ============================================================================
# Tom Slee, 2013--2017.
#
# An ABListing represents and individual Airbnb listing
# ============================================================================
import logging
import re
from lxml import html
import psycopg2
import json
import airbnb_ws

# Set up logging
# logger = logging.getLogger(__name__)
logger = logging.getLogger("airbnb")
logger.setLevel(logging.INFO)


class ABListing():
    """
    # ABListing represents an Airbnb room_id, as captured at a moment in time.
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
                if insert_replace_flag == self.config.FLAGS_INSERT_REPLACE:
                    rowcount = self.__update()
                if (rowcount == 0 or
                        insert_replace_flag == self.config.FLAGS_INSERT_NO_REPLACE):
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
            room_url = self.config.URL_ROOM_ROOT + str(self.room_id)
            response = airbnb_ws.ws_request_with_repeats(self.config, room_url)
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
                self.coworker_hosted, self.extra_host_languages, self.name,
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

            if flag == self.config.FLAGS_ADD:
                self.save(self.config.FLAGS_INSERT_REPLACE)
            elif flag == self.config.FLAGS_PRINT:
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
