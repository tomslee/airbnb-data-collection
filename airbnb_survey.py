#!/usr/bin/python3
# ============================================================================
# Tom Slee, 2013--2017.
#
# An ABSurvey is a scrape of the Airbnb web site, which may also collect
# information about listings. There are several survey types:
# - neighborhood (-s)
# - bounding box (-sb)
# - zipcode (-sz)
# See the README for which to use.
# ============================================================================
import logging
import sys
import random
import psycopg2
from datetime import date
from airbnb_listing import ABListing
import airbnb_ws

# Set up logging
# logger = logging.getLogger(__name__)
logger = logging.getLogger("airbnb")
logger.setLevel(logging.INFO)


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
        response = airbnb_ws.ws_request_with_repeats(survey.config, survey.config.URL_API_SEARCH_ROOT, params)
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
                    if flag == survey.config.FLAGS_ADD:
                        if listing.save(survey.config.FLAGS_INSERT_NO_REPLACE):
                            new_rooms += 1
                    elif flag == survey.config.FLAGS_PRINT:
                        print(room_type, listing.room_id)
        if room_count > 0:
            has_rooms = 1
        else:
            has_rooms = 0
        if flag == survey.config.FLAGS_ADD:
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
        if search_by == config.SEARCH_BY_NEIGHBORHOOD:
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
            response = airbnb_ws.ws_request_with_repeats(survey.config, survey.config.URL_API_SEARCH_ROOT, params)
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
                        if flag == survey.config.FLAGS_ADD:
                            if listing.save(survey.config.FLAGS_INSERT_NO_REPLACE):
                                new_rooms += 1
                        elif flag == survey.config.FLAGS_PRINT:
                            print(room_type, listing.room_id)

            if flag == survey.config.FLAGS_PRINT:
                # for FLAGS_PRINT, fetch one page and print it
                sys.exit(0)
            if room_count < survey.config.SEARCH_LISTINGS_ON_FULL_PAGE:
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
            response = airbnb_ws.ws_request_with_repeats(survey.config, survey.config.URL_API_SEARCH_ROOT, params)
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
                    listing = ABListing(room_id, survey.survey_id, room_type)
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
                    if flag == survey.config.FLAGS_ADD:
                        if listing.save(survey.config.FLAGS_INSERT_NO_REPLACE):
                            new_rooms += 1
                    elif flag == survey.config.FLAGS_PRINT:
                        print(room_type, listing.room_id)
            if flag == survey.config.FLAGS_PRINT:
                # for FLAGS_PRINT, fetch one page and print it
                sys.exit(0)
            page_number += 1
            # log progress on this survey in DB - survey id, room_type, zoomstack, page_number are the important ones
            survey.log_progress_bounding_box(room_type, guests, zoomstack, page_number, room_count)

            if room_count < survey.config.SEARCH_LISTINGS_ON_FULL_PAGE:
                logger.debug("Final page of listings for this search")
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


class ABSurvey():

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
                listing = ABListing(self.config, room_id, self.survey_id)
                if room_id is None:
                    break
                else:
                    if listing.ws_get_room_info(self.config.FLAGS_ADD):
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
        if self.search_area_name == self.config.SEARCH_AREA_GLOBAL:
            # "Special case": global search
            self.__global_search()
        else:
            if search_by == self.config.SEARCH_BY_BOUNDING_BOX:
                logger.info("Searching by bounding box")
                self.__search_loop_bounding_box(flag)
                # logger.info("Searching by bounding box - logged")
                # self.__search_loop_bounding_box_logged(flag)
                pass
            elif search_by == self.config.SEARCH_BY_ZIPCODE:
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
            if rectangle_zoom < max(1, (self.config.SEARCH_MAX_RECTANGLE_ZOOM - 2 * (guests - 1))):
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
                blur = abs(n_lat - s_lat) * self.config.SEARCH_RECTANGLE_EDGE_BLUR
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
            if flag == self.config.FLAGS_PRINT:
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
                    if flag != self.config.FLAGS_PRINT:
                        # this efficiency check can be implemented later
                        count = page_has_been_retrieved(
                            self.config, survey_id, room_type, str(zipcode),
                            guests, page_number, self.config.SEARCH_BY_ZIPCODE)
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
                    if flag == self.config.FLAGS_PRINT:
                        # for FLAGS_PRINT, fetch one page and print it
                        sys.exit(0)
                    if room_count < self.config.SEARCH_LISTINGS_ON_FULL_PAGE:
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
            blur = abs(n_lat - s_lat) * self.config.SEARCH_RECTANGLE_EDGE_BLUR
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

        if flag == self.config.FLAGS_PRINT:
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
                    if flag != self.config.FLAGS_PRINT:
                        count = page_has_been_retrieved(
                            self.config, self.survey_id, room_type,
                            neighborhood, guests, page_number,
                            self.config.SEARCH_BY_NEIGHBORHOOD)
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
                                     neighborhood=neighborhood,
                                     room_count=room_count,
                                     page_number=str(page_number)))

                    if flag == self.config.FLAGS_PRINT:
                        # for FLAGS_PRINT, fetch one page and print it
                        sys.exit(0)
                    if room_count < self.config.SEARCH_LISTINGS_ON_FULL_PAGE:
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
            response = airbnb_ws.ws_request_with_repeats(self.config, self.config.URL_API_SEARCH_ROOT, params)
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
                        if flag == self.config.FLAGS_ADD:
                            if listing.save(self.config.FLAGS_INSERT_NO_REPLACE):
                                new_rooms += 1
                        elif flag == self.config.FLAGS_PRINT:
                            print(room_type, listing.room_id)
            if room_count > 0:
                has_rooms = 1
            else:
                has_rooms = 0
            if flag == self.config.FLAGS_ADD:
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
            json_listing = result["listing"]
            json_pricing = result["pricing_quote"]
            listing = ABListing(self.config, room_id, survey.survey_id, room_type)
            listing.host_id = json_listing["primary_host"]["id"] if "primary_host" in json_listing else None
            listing.address = json_listing["public_address"] if "public_address" in json_listing else None
            listing.reviews = json_listing["reviews_count"] if "reviews_count" in json_listing else None
            listing.overall_satisfaction = json_listing["star_rating"] if "star_rating" in json_listing else None
            listing.accommodates = json_listing["person_capacity"] if "person_capacity" in json_listing else None
            listing.bedrooms = json_listing["bedrooms"] if "bedrooms" in json_listing else None
            listing.price = json_pricing["rate"]["amount"] if "rate" in json_listing else None
            listing.latitude = json_listing["lat"] if "lat" in json_listing else None
            listing.longitude = json_listing["lng"] if "lng" in json_listing else None
            listing.coworker_hosted = json_listing["coworker_hosted"] if "coworker_hosted" in json_listing else None
            listing.extra_host_languages = json_listing["extra_host_languages"] \
                if "extra_host_languages" in json_listing else None
            listing.name = json_listing["name"] if "name" in json_listing else None
            listing.property_type = json_listing["property_type"] if "property_type" in json_listing else None
            listing.currency = json_pricing["rate"]["currency"] if "rate" in json_pricing else None
            listing.rate_type = json_pricing["rate_type"] if "rate_type" in json_pricing else None
            return listing
        except:
            logger.exception("Error in survey.listing_from_search_page_json: returning None")
            sys.exit(-1)
            return None
