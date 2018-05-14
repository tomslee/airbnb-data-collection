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
import time
from datetime import date
from bs4 import BeautifulSoup
import json
from airbnb_listing import ABListing
import airbnb_ws

logger = logging.getLogger()

class Timer:
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start

class ABSurvey():
    """
    Class to represent a generic survey, using one of several methods.
    Specific surveys (eg bounding box, neighbourhood) are implemented in
    subclasses. Right now (May 2018), however, only the bounding box survey
    is working.
    """

    def __init__(self, config, survey_id):
        self.config = config
        self.survey_id = survey_id
        self.search_area_id = None
        self.search_area_name = None
        self.set_search_area()
        self.room_types = ["Private room", "Entire home/apt", "Shared room"]

        # Set up logging
        logger.setLevel(config.log_level)

        # create a file handler
        logfile = "survey-{survey_id}.log".format(survey_id=self.survey_id)
        filelog_handler = logging.FileHandler(logfile, encoding="utf-8")
        filelog_handler.setLevel(config.log_level)
        filelog_formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s%(message)s')
        filelog_handler.setFormatter(filelog_formatter)

        # logging: set log file name, format, and level
        logger.addHandler(filelog_handler)

        # Suppress informational logging from requests module
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logger.propagate = False

    def set_search_area(self):
        """
        Compute the search area ID and name.
        """
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
        except (KeyboardInterrupt, SystemExit):
            cur.close()
            raise
        except Exception:
            cur.close()
            logger.error("No search area for survey_id " + str(self.survey_id))
            raise

    def update_survey_entry(self, search_by):
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

    def listing_from_search_page_json(self, json, room_id):
        try:
            listing = ABListing(self.config, room_id, self.survey_id)
            # listing
            json_listing = json["listing"] if "listing" in json else None
            if json_listing is None:
                return None
            if "room_type" in json_listing:
                listing.room_type = json_listing["room_type"]
            else:
                listing.room_type = None
            if "user" in json_listing:
                listing.host_id = json_listing["user"]["id"]
            else:
                listing.host_id = None
            if "public_address" in json_listing:
                listing.address = json_listing["public_address"]
            else:
                listing.address = None
            if "reviews_count" in json_listing:
                listing.reviews = json_listing["reviews_count"]
            else:
                listing.reviews = None
            if "star_rating" in json_listing:
                listing.overall_satisfaction = json_listing["star_rating"]
            else:
                listing.overall_satisfaction = None
            if "person_capacity" in json_listing:
                listing.accommodates = json_listing["person_capacity"]
            else:
                listing.accommodates = None
            if "bedrooms" in json_listing:
                listing.bedrooms = json_listing["bedrooms"]
            else:
                listing.bedrooms = None
            if "bathrooms" in json_listing:
                listing.bathrooms = json_listing["bathrooms"]
            else:
                listing.bathrooms = None
            if "lat" in json_listing:
                listing.latitude = json_listing["lat"]
            else:
                listing.latitude = None
            if "lng" in json_listing:
                listing.longitude = json_listing["lng"]
            else:
                listing.longitude = None
            # The coworker_hosted item is missing or elsewhere
            listing.coworker_hosted = json_listing["coworker_hosted"] \
                    if "coworker_hosted" in json_listing else None
            # The extra_host_language item is missing or elsewhere
            listing.extra_host_languages = json_listing["extra_host_languages"] \
                if "extra_host_languages" in json_listing else None
            listing.name = json_listing["name"] \
                    if "name" in json_listing else None
            listing.property_type = json_listing["property_type"] \
                    if "property_type" in json_listing else None
            # pricing
            json_pricing = json["pricing_quote"]
            listing.price = json_pricing["rate"]["amount"] if "rate" in json_pricing else None
            listing.currency = json_pricing["rate"]["currency"] if "rate" in json_pricing else None
            listing.rate_type = json_pricing["rate_type"] if "rate_type" in json_pricing else None
            return listing
        except:
            logger.exception("Error in survey.listing_from_search_page_json: returning None")
            sys.exit(-1)
            return None

    def log_progress(self, room_type, neighborhood_id,
                     guests, section_offset, has_rooms):
        """ Add an entry to the survey_progress_log table to record the fact
        that a page has been visited.
        This does not apply to search by bounding box, but does apply to both
        neighborhood and zipcode searches, which is why it is in ABSurvey.
        """
        try:
            page_info = (self.survey_id, room_type, neighborhood_id,
                         guests, section_offset, has_rooms)
            logger.debug("Search page: " + str(page_info))
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

    def fini(self):
        """
        Wrap up a survey: correcting status and survey_date
        """
        try:
            logger.info("Finishing survey %s, for %s",
                        self.survey_id, self.search_area_name)
            sql_update = """
            update survey
            set survey_date = (
            select min(last_modified)
            from room
            where room.survey_id = survey.survey_id
            ), status = 1
            where survey_id = %s
            """
            conn = self.config.connect()
            cur = conn.cursor()
            cur.execute(sql_update, (self.survey_id, ))
            cur.close()
            conn.commit()
            return True
        except:
            logger.exception("Survey fini failed")
            return False

    def page_has_been_retrieved(self, room_type, neighborhood_or_zipcode,
                                guests, page_number, search_by):
        """
        Used with neighborhood and zipcode logging (see method above).
        Returns 1 if the page has been retrieved previously and has rooms
        Returns 0 if the page has been retrieved previously and has no rooms
        Returns -1 if the page has not been retrieved previously
        """
        conn = self.config.connect()
        cur = conn.cursor()
        has_rooms = 0
        try:
            if search_by == self.config.SEARCH_BY_NEIGHBORHOOD:
                neighborhood = neighborhood_or_zipcode
                # TODO: Currently fails when there are no neighborhoods
                if neighborhood is None:
                    has_rooms = -1
                else:
                    params = (self.survey_id, room_type, neighborhood, guests,
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
                    logger.debug("has_rooms = %s for neighborhood %s",
                                 str(has_rooms), neighborhood)
            else:  # SEARCH_BY_ZIPCODE
                zipcode = int(neighborhood_or_zipcode)
                params = (self.survey_id, room_type, zipcode, guests, page_number,)
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
                logger.debug("has_rooms = %s for zipcode %s",
                             str(has_rooms), str(zipcode))
        except Exception:
            has_rooms = -1
            logger.debug("Page has not been retrieved previously")
        finally:
            cur.close()
            return has_rooms



class ABSurveyByBoundingBox(ABSurvey):
    """
    Subclass of Survey that carries out a survey by a quadtree of bounding
    boxes: recursively searching rectangles.
    """


    def __init__(self, config, survey_id):
        super().__init__(config, survey_id)
        self.search_node_counter = 0
        self.logged_progress = self.get_logged_progress()
        self.bounding_box = self.get_bounding_box()

    def get_logged_progress(self):
        """
        Retrieve from the database the progress logged in previous attempts to
        carry out this survey, to pick up where we left off.
        Returns None if there is no progress logged.
        """
        try:
            sql = """
            select room_type, quadtree_node, median_node
            from survey_progress_log_bb
            where survey_id = %s
            """
            conn = self.config.connect()
            cur = conn.cursor()
            cur.execute(sql, (self.survey_id,))
            row = cur.fetchone()
            cur.close()
            conn.commit()
            if row is None:
                logger.debug("No progress logged for survey %s", self.survey_id)
                self.logged_progress = None
            else:
                logged_progress = {}
                logged_progress["room_type"] = row[0]
                logged_progress["quadtree"] = eval(row[1])
                logged_progress["median"] = eval(row[2])
                logger.info("Resuming survey - retrieved logged progress")
                logger.info("\troom_type=%s", logged_progress["room_type"])
                logger.info("\tquadtree node=%s", logged_progress["quadtree"])
                logger.info("\tmedian node=%s", logged_progress["median"])
                return logged_progress
        except Exception:
            logger.exception("Exception in get_progress: setting logged progress to None")
            return None

    def get_bounding_box(self):
        try:
            # Get the bounding box
            conn = self.config.connect()
            cur = conn.cursor()
            cur.execute("""
                        select bb_n_lat, bb_e_lng, bb_s_lat, bb_w_lng
                        from search_area sa join survey s
                        on sa.search_area_id = s.search_area_id
                        where s.survey_id = %s""", (self.survey_id,))
            # result comes back as a tuple. We want it mutable later, so
            # convert to a list [n_lat, e_lng, s_lat, w_lng]
            bounding_box = list(cur.fetchone())
            cur.close()
            # Validate the bounding box
            if None in bounding_box:
                logger.error("Invalid bounding box: contains 'None'")
                return None
            if bounding_box[0] <= bounding_box[2]:
                logger.error("Invalid bounding box: n_lat must be > s_lat")
                return None
            if bounding_box[1] <= bounding_box[3]:
                logger.error("Invalid bounding box: e_lng must be > w_lng")
                return None
            return bounding_box
        except Exception:
            logger.exception("Exception in set_bounding_box")
            self.bounding_box = None

    def search(self, flag):
        """
        Initialize bounding box search.
        A bounding box is a rectangle around a city, specified in the
        search_area table. The loop goes to quadrants of the bounding box
        rectangle and, if new listings are found, breaks that rectangle
        into four quadrants and tries again, recursively.
        The rectangles, including the bounding box, are represented by
        [n_lat, e_lng, s_lat, w_lng], because Airbnb uses the SW and NE
        corners of the box.
        """
        try:
            logger.info("=" * 70)
            logger.info("Survey {survey_id}, for {search_area_name}".format(
                survey_id=self.survey_id, search_area_name=self.search_area_name
            ))
            ABSurvey.update_survey_entry(self, self.config.SEARCH_BY_BOUNDING_BOX)
            logger.info("Searching by bounding box, max_zoom=%s",
                        self.config.SEARCH_MAX_RECTANGLE_ZOOM)
            # Initialize search parameters
            # quadtree_node holds the quadtree: each rectangle is
            # divided into 00 | 01 | 10 | 11, and the next level down adds
            # set starting point
            quadtree_node = [] # list of [0,0] etc coordinates
            median_node = [] # median lat, long to define optimal quadrants
            # set starting point for survey being resumed
            if self.logged_progress:
                logger.info("Restarting incomplete survey")
            if self.config.SEARCH_DO_LOOP_OVER_ROOM_TYPES:
                for room_type in self.room_types:
                    logger.info("-" * 70)
                    logger.info("Beginning of search for %s", room_type)
                    self.recurse_quadtree(quadtree_node, median_node, room_type, flag)
            else:
                self.recurse_quadtree(quadtree_node, median_node, None, flag)
            self.fini()
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception:
            logger.exception("Error")

    def recurse_quadtree(self, quadtree_node, median_node, room_type, flag):
        """
        Recursive function to search for listings inside a rectangle.
        The actual search calls are done in search_node, and
        this method (recurse_quadtree) prints output and sets up new
        rectangles, if necessary, for another round of searching.

        To match Airbnb's use of SW and NE corners, quadrants are divided
        like this:

                     [0, 1] (NW)   |   [0, 0] (NE)
                     -----------------------------
                     [1, 1] (SW)   |   [1, 0] (SE)

        The quadrants are searched in the order [0,0], [0,1], [1,0], [1,1]
        """
        try:
            zoomable = True
            if self.is_subtree_previously_completed(quadtree_node, room_type):
                logger.info("Resuming survey: subtree previously completed: %s", quadtree_node)
                # This node is part of a tree that has already been searched
                # completely in a previous attempt to run this survey.
                # Go immediately to the next quadrant at the current level,
                # or (if this is a [1, 1] node) go back up the tree one level.
                # For example: if quadtree_node is [0,0] and the logged
                # progress is [1,0] then the subtree for [0,0] is completed. If
                # progress is [0,0][0,1] then the subtree is not completed.
                # TODO: use the same technique as the loop, below
                if quadtree_node[-1] == [0, 0]:
                    quadtree_node[-1] = [0, 1]
                elif quadtree_node[-1] == [0, 1]:
                    quadtree_node[-1] = [1, 0]
                elif quadtree_node[-1] == [1, 0]:
                    quadtree_node[-1] = [1, 1]
                elif quadtree_node[-1] == [1, 1]:
                    del quadtree_node[-1]
                return

            # The subtree for this node has not been searched completely, so we
            # will continue to explore the tree. But does the current node need
            # to be searched? Only if it is at least as far down the tree as
            # the logged progress.
            # TODO Currently the most recent quadrant is searched again: this
            # is not a big problem.
            searchable_node = (
                self.logged_progress is None
                or len(quadtree_node) >= len(self.logged_progress["quadtree"]))
            if searchable_node:
                # The logged_progress can be set to None, as the survey is now
                # resumed. This should be done only once, but it is repeated.
                # Still, it is cheap.
                self.logged_progress = None
                (zoomable, median_leaf) = self.search_node(
                    quadtree_node, median_node, room_type, flag)
            else:
                median_leaf = self.logged_progress["median"][-1]
                logger.info("Resuming survey: node previously searched: %s", quadtree_node)

            # Recurse through the tree
            if zoomable:
                # and len(self.logged_progress["quadtree"]) >= len(quadtree_node)):
                # append a node to the quadtree for a new level
                quadtree_node.append([0,0])
                median_node.append(median_leaf)
                for int_leaf in range(4):
                    # Loop over [0,0], [0,1], [1,0], [1,1]
                    quadtree_leaf = [int(i)
                                     for i in str(bin(int_leaf))[2:].zfill(2)]
                    quadtree_node[-1] = quadtree_leaf
                    self.recurse_quadtree(quadtree_node, median_node,
                                          room_type, flag)
                # the search of the quadtree below this node is complete:
                # remove the leaf element from the tree and return to go up a level
                if len(quadtree_node) > 0:
                    del quadtree_node[-1]
                if len(median_node) > 0:
                    del median_node[-1]
            logger.debug("Returning from recurse_quadtree for %s", quadtree_node)
            if flag == self.config.FLAGS_PRINT:
                # for FLAGS_PRINT, fetch one page and print it
                sys.exit(0)
        except (SystemExit, KeyboardInterrupt):
            raise
        except TypeError as type_error:
            logger.exception("TypeError in recurse_quadtree")
            logger.error(type_error.args)
            raise
        except:
            logger.exception("Error in recurse_quadtree")
            raise

    def search_node(self, quadtree_node, median_node, room_type, flag):
        """
            rectangle is (n_lat, e_lng, s_lat, w_lng)
            returns number of *new* rooms and number of pages tested
        """
        try:
            logger.info("-" * 70)
            rectangle = self.get_rectangle_from_quadtree_node(quadtree_node, median_node)
            logger.info("Searching rectangle: zoom factor = %s, node = %s",
                        len(quadtree_node), str(quadtree_node))
            logger.debug("Rectangle: N={n:+.5f}, E={e:+.5f}, S={s:+.5f}, W={w:+.5f}"
                         .format(n=rectangle[0], e=rectangle[1],
                                 s=rectangle[2], w=rectangle[3]))
            new_rooms = 0
            # set zoomable to false if the search finishes without returning a
            # full complement of 20 pages, 18 listings per page
            zoomable = True

            # median_lists are collected from results on each page and used to
            # calculate the median values, which will be used to divide the 
            # volume into optimal "quadrants".
            median_lists = {}
            median_lists["latitude"] = []
            median_lists["longitude"] = []
            for section_offset in range(0, self.config.SEARCH_MAX_PAGES):
                self.search_node_counter += 1
                # section_offset is the zero-based counter used on the site
                # page number is convenient for logging, etc
                page_number = section_offset + 1
                room_count = 0

                if self.config.API_KEY:
                    # API (returns JSON)
                    # set up the parameters for the request
                    logger.debug("API key found: using API search at %s",
                                 self.config.URL_API_SEARCH_ROOT)
                    params = {}
                    params["version"] = "1.3.5"
                    params["_format"] = "for_explore_search_web"
                    params["experiences_per_grid"] = str(20)
                    params["items_per_grid"] = str(18)
                    params["guidebooks_per_grid"] = str(20)
                    params["auto_ib"] = str(True)
                    params["fetch_filters"] = str(True)
                    params["has_zero_guest_treatment"] = str(True)
                    params["is_guided_search"] = str(True)
                    params["is_new_cards_experiment"] = str(True)
                    params["luxury_pre_launch"] = str(False)
                    params["query_understanding_enabled"] = str(True)
                    params["show_groupings"] = str(True)
                    params["supports_for_you_v3"] = str(True)
                    params["timezone_offset"] = "-240"
                    params["metadata_only"] = str(False)
                    params["is_standard_search"] = str(True)
                    params["refinement_paths[]"] = "/homes"
                    params["selected_tab_id"] = "home_tab"
                    params["allow_override[]"] = ""
                    if self.config.SEARCH_DO_LOOP_OVER_ROOM_TYPES:
                        params["room_types[]"] = room_type
                    params["ne_lat"] = str(rectangle[0])
                    params["ne_lng"] = str(rectangle[1])
                    params["sw_lat"] = str(rectangle[2])
                    params["sw_lng"] = str(rectangle[3])
                    params["search_by_map"] = str(True)
                    params["screen_size"] = "medium"
                    params["_intents"] = "p1"
                    params["key"] = self.config.API_KEY
                    params["client_session_id"] = self.config.CLIENT_SESSION_ID
                    # params["zoom"] = str(True)
                    # params["federated_search_session_id"] = "45de42ea-60d4-49a9-9335-9e52789cd306"
                    # params["query"] = "Lisbon Portugal"
                    # params["currency"] = "CAD"
                    # params["locale"] = "en-CA"
                    if section_offset > 0:
                        params["section_offset"] = str(section_offset)
                    # make the http request
                    response = airbnb_ws.ws_request_with_repeats(
                        self.config, self.config.URL_API_SEARCH_ROOT, params)
                    # process the response
                    if response:
                        json_doc = json.loads(response.text)
                    else:
                        # If no response, maybe it's a network problem rather
                        # than a lack of data.To be conservative go to the next page
                        # rather than the next rectangle
                        logger.warning(
                            "No response received from request despite multiple attempts: %s",
                            params)
                        continue
                else:
                    # Web page (returns HTML)
                    logger.debug("No API key found in config file: using web search at %s",
                                 self.config.URL_API_SEARCH_ROOT)
                    params = {}
                    params["source"] = "filter"
                    params["_format"] = "for_explore_search_web"
                    params["experiences_per_grid"] = str(20)
                    params["items_per_grid"] = str(18)
                    params["guidebooks_per_grid"] = str(20)
                    params["auto_ib"] = str(True)
                    params["fetch_filters"] = str(True)
                    params["has_zero_guest_treatment"] = str(True)
                    params["is_guided_search"] = str(True)
                    params["is_new_cards_experiment"] = str(True)
                    params["luxury_pre_launch"] = str(False)
                    params["query_understanding_enabled"] = str(True)
                    params["show_groupings"] = str(True)
                    params["supports_for_you_v3"] = str(True)
                    params["timezone_offset"] = "-240"
                    params["metadata_only"] = str(False)
                    params["is_standard_search"] = str(True)
                    params["refinement_paths[]"] = "/homes"
                    params["selected_tab_id"] = "home_tab"
                    params["allow_override[]"] = ""
                    params["ne_lat"] = str(rectangle[0])
                    params["ne_lng"] = str(rectangle[1])
                    params["sw_lat"] = str(rectangle[2])
                    params["sw_lng"] = str(rectangle[3])
                    params["search_by_map"] = str(True)
                    params["screen_size"] = "medium"
                    if section_offset > 0:
                        params["section_offset"] = str(section_offset)
                    # make the http request
                    response = airbnb_ws.ws_request_with_repeats(
                        self.config, self.config.URL_API_SEARCH_ROOT, params)
                    # process the response
                    if not response:
                        # If no response, maybe it's a network problem rather
                        # than a lack of data. To be conservative go to the next page
                        # rather than the next rectangle
                        logger.warning(
                            "No response received from request despite multiple attempts: %s",
                            params)
                        continue
                    soup = BeautifulSoup(response.content.decode("utf-8",
                                                                 "ignore"),
                                         "lxml")
                    html_file = open("test.html", mode="w", encoding="utf-8")
                    html_file.write(soup.prettify())
                    html_file.close()
                    # The returned page includes a script tag that encloses a
                    # comment. The comment in turn includes a complex json
                    # structure as a string, which has the data we need
                    spaspabundlejs_set = soup.find_all("script",
                                                       {"type": "application/json",
                                                        "data-hypernova-key": "spaspabundlejs"})
                    if spaspabundlejs_set:
                        logger.debug("Found spaspabundlejs tag")
                        comment = spaspabundlejs_set[0].contents[0]
                        # strip out the comment tags (everything outside the
                        # outermost curly braces)
                        json_doc = json.loads(comment[comment.find("{"):comment.rfind("}")+1])
                        logger.debug("results-containing json found")
                    else:
                        logger.warning("json results-containing script node "
                                       "(spaspabundlejs) not found in the web page: "
                                       "go to next page")
                        return None
                # Now we have the json. It includes a list of 18 or fewer listings
                if logger.isEnabledFor(logging.DEBUG):
                    json_file = open(
                        "json_listing_{}.json".format(self.search_node_counter),
                        mode="w", encoding="utf-8")
                    json_file.write(json.dumps(json_doc, indent=4, sort_keys=True))
                    json_file.close()
                # Steal a function from StackOverflow which searches for items
                # with a given list of keys (in this case just one: "listing")
                # https://stackoverflow.com/questions/14048948/how-to-find-a-particular-json-value-by-key
                def search_json_keys(key, json_doc):
                    """ Return a list of the values for each occurrence of key
                    in json_doc, at all levels. In particular, "listings"
                    occurs more than once, and we need to get them all."""
                    found = []
                    if isinstance(json_doc, dict):
                        if key in json_doc.keys():
                            found.append(json_doc[key])
                        elif json_doc.keys():
                            for json_key in json_doc.keys():
                                result_list = search_json_keys(key, json_doc[json_key])
                                if result_list:
                                    found.extend(result_list)
                    elif isinstance(json_doc, list):
                        for item in json_doc:
                            result_list = search_json_keys(key, item)
                            if result_list:
                                found.extend(result_list)
                    return found

                # Get all items with tags "listings". Each json_listings is a
                # list, and each json_listing is a {listing, pricing_quote, verified}
                # dict for the listing in question
                # There may be multiple lists of listings
                json_listings_lists = search_json_keys("listings", json_doc)
                # json_doc = json_doc["explore_tabs"]
                # if json_doc: logger.debug("json: explore_tabs")
                # json_doc = json_doc["sections"]
                # if json_doc: logger.debug("json: sections")

                if json_listings_lists is not None:
                    room_count = 0
                    for json_listings in json_listings_lists:
                        if json_listings is None:
                            continue
                        for json_listing in json_listings:
                            room_id = int(json_listing["listing"]["id"])
                            if room_id is not None:
                                room_count += 1
                                listing = self.listing_from_search_page_json(json_listing, room_id)
                                if listing is None:
                                    continue
                                if listing.latitude is not None:
                                    median_lists["latitude"].append(listing.latitude)
                                if listing.longitude is not None:
                                    median_lists["longitude"].append(listing.longitude)
                                if listing.host_id is not None:
                                    listing.deleted = 0
                                    if flag == self.config.FLAGS_ADD:
                                        if listing.save(self.config.FLAGS_INSERT_NO_REPLACE):
                                            new_rooms += 1
                                    elif flag == self.config.FLAGS_PRINT:
                                        print(listing.room_type, listing.room_id)

                # Log page-level results
                logger.info("Page {page_number:02d} returned {room_count:02d} listings"
                        .format(page_number=page_number, room_count=room_count))
                if flag == self.config.FLAGS_PRINT:
                    # for FLAGS_PRINT, fetch one page and print it
                    sys.exit(0)
                if room_count < self.config.SEARCH_LISTINGS_ON_FULL_PAGE:
                    # If a full page of listings is not returned by Airbnb,
                    # this branch of the search is complete.
                    logger.info("Final page of listings for this search")
                    zoomable = False
                    break
            # Log node-level results
            if self.config.SEARCH_DO_LOOP_OVER_ROOM_TYPES:
                logger.info("Results: %s pages, %s new %s listings.",
                            page_number, new_rooms, room_type)
            else:
                logger.info("Results: %s pages, %s new rooms",
                            page_number, new_rooms)



            # Median-based partitioning not currently in use: may use later
            if len(median_node) == 0:
                median_leaf = "[]"
            else:
                median_leaf = median_node[-1]
            # calculate medians
            if room_count > 0:
                median_lat = round(sorted(median_lists["latitude"])
                                   [int(len(median_lists["latitude"])/2)], 5
                                  )
                median_lng = round(sorted(median_lists["longitude"])
                                   [int(len(median_lists["longitude"])/2)], 5
                                  )
                median_leaf = [median_lat, median_lng]
            else:
                # values not needed, but we need to fill in an item anyway
                median_leaf = [0, 0]
            # log progress
            self.log_progress(room_type, quadtree_node, median_node)
            return (zoomable, median_leaf)
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

    def get_rectangle_from_quadtree_node(self, quadtree_node, median_node):
        try:
            rectangle = self.bounding_box[0:4]
            for node, medians in zip(quadtree_node, median_node):
                logger.debug("Quadtrees: %s", node)
                logger.debug("Medians: %s", medians)
                [n_lat, e_lng, s_lat, w_lng] = rectangle
                blur = abs(n_lat - s_lat) * self.config.SEARCH_RECTANGLE_EDGE_BLUR
                # find the mindpoints of the rectangle
                mid_lat = (n_lat + s_lat)/2.0
                mid_lng = (e_lng + w_lng)/2.0
                # mid_lat = medians[0]
                # mid_lng = medians[1]
                # overlap quadrants to ensure coverage at high zoom levels
                # Airbnb max zoom (18) is about 0.004 on a side.
                rectangle = []
                if node == [0, 0]: # NE
                    rectangle = [round(n_lat + blur, 5),
                                 round(e_lng + blur, 5),
                                 round(mid_lat - blur, 5),
                                 round(mid_lng - blur, 5),]
                elif node == [0, 1]: # NW
                    rectangle = [round(n_lat + blur, 5),
                                 round(mid_lng + blur, 5),
                                 round(mid_lat - blur, 5),
                                 round(w_lng - blur, 5),]
                elif node == [1, 0]: # SE
                    rectangle = [round(mid_lat + blur, 5),
                                 round(e_lng + blur, 5),
                                 round(s_lat - blur, 5),
                                 round(mid_lng - blur, 5),]
                elif node == [1, 1]: # SW
                    rectangle = [round(mid_lat + blur, 5),
                                 round(mid_lng + blur, 5),
                                 round(s_lat - blur, 5),
                                 round(w_lng - blur, 5),]
            logger.info("Rectangle calculated: %s", rectangle)
            return rectangle
        except:
            logger.exception("Exception in get_rectangle_from_quadtree_node")
            return None

    def is_subtree_previously_completed(self, quadtree_node, room_type):
        """
        Return True if the child subtree of this node was completed
        in a previous attempt at this survey.
        """
        subtree_previously_completed = False
        if self.logged_progress:
            # Compare the current node to the logged progress node by
            # converting into strings, then comparing the integer value.
            if (self.room_types.index(room_type)
                < self.room_types.index(self.logged_progress["room_type"])):
                subtree_previously_completed = True
                return subtree_previously_completed
            if (self.room_types.index(room_type)
                > self.room_types.index(self.logged_progress["room_type"])):
                subtree_previously_completed = False
                return subtree_previously_completed
            common_length = min(len(quadtree_node),
                                len(self.logged_progress["quadtree"]))
            s_this_quadrant = ''.join(str(quadtree_node[i][j])
                                      for j in range(0, 2)
                                      for i in range(0, common_length))
            s_logged_progress = ''.join(
                str(self.logged_progress["quadtree"][i][j])
                for j in range(0, 2)
                for i in range(0, common_length))
            if (s_this_quadrant != ""
                and int(s_this_quadrant) < int(s_logged_progress)):
                subtree_previously_completed = True
        return subtree_previously_completed


    def log_progress(self, room_type, quadtree_node, median_node):
        try:
            # This upsert statement requires PostgreSQL 9.5
            # Convert the quadrant to a string with repr() before storing it
            sql = """
            insert into survey_progress_log_bb
            (survey_id, room_type, quadtree_node, median_node)
            values (%s, %s, %s, %s)
            on conflict ON CONSTRAINT survey_progress_log_bb_pkey
            do update
                set room_type = %s, quadtree_node = %s, median_node = %s
                , last_modified = now()
            where survey_progress_log_bb.survey_id = %s
            """
            conn = self.config.connect()
            cur = conn.cursor()
            cur.execute(sql, (self.survey_id,
                              room_type, repr(quadtree_node), repr(median_node),
                              room_type, repr(quadtree_node), repr(median_node),
                              self.survey_id))
            cur.close()
            conn.commit()
            logger.debug("Progress logged")
            return True
        except Exception as e:
            logger.warning("""Progress not logged: survey not affected, but
                    resume will not be available if survey is truncated.""")
            logger.exception("Exception in log_progress: {e}".format(e=type(e)))
            conn.close()
            return False



class ABSurveyByNeighborhood(ABSurvey):
    """
    Subclass of Survey that carries out a survey by looping over
    the neighborhoods as defined on the Airbnb web site.
    """

    def search(self, flag):
        logger.info("=" * 70)
        logger.info("Survey {survey_id}, for {search_area_name}".format(
            survey_id=self.survey_id, search_area_name=self.search_area_name
        ))
        ABSurvey.update_survey_entry(self, self.config.SEARCH_BY_NEIGHBORHOOD)
        if self.search_area_name == self.config.SEARCH_AREA_GLOBAL:
            # "Special case": global search
            # self.__global_search()
            logger.error("Global search not currently implemented")
        else:
            logger.info("Searching by neighborhood")
            neighborhoods = self.get_neighborhoods_from_search_area()
            # for some cities (eg Havana) the neighborhood information
            # is incomplete, and an additional search with no
            # neighborhood is useful
            neighborhoods = neighborhoods + [None]
            for room_type in self.room_types:
                logger.debug(
                    "Searching for %(rt)s by neighborhood",
                    {"rt": room_type})
                if len(neighborhoods) > 0:
                    self.__search_loop_neighborhoods(neighborhoods,
                                                     room_type, flag)
                else:
                    self.__search_neighborhood(None, room_type, flag)
        self.fini()

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
                max_guests = min(4, self.config.SEARCH_MAX_GUESTS)
            else:
                max_guests = self.config.SEARCH_MAX_GUESTS
            for guests in range(1, max_guests):
                logger.debug("Searching for %(g)i guests", {"g": guests})
                for section_offset in range(0, self.config.SEARCH_MAX_PAGES):
                    if flag != self.config.FLAGS_PRINT:
                        count = self.page_has_been_retrieved(
                            room_type, neighborhood, guests, section_offset,
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
                        room_type, neighborhood, guests, section_offset, flag)
                    logger.info(("{room_type} ({g} guests): neighborhood {neighborhood}: "
                                 "{room_count} rooms, {section_offset} pages").format(
                                     room_type=room_type, g=str(guests),
                                     neighborhood=neighborhood,
                                     room_count=room_count,
                                     section_offset=str(section_offset + 1)))
                    if flag == self.config.FLAGS_PRINT:
                        # for FLAGS_PRINT, fetch one page and print it
                        sys.exit(0)
                    if room_count < self.config.SEARCH_LISTINGS_ON_FULL_PAGE:
                        logger.debug("Final page of listings for this search")
                        break
        except Exception:
            raise

    def __search_neighborhood_page(self, room_type, neighborhood, guests,
                                   section_offset, flag):
        try:
            logger.info("-" * 70)
            logger.info(room_type + ", " +
                        str(neighborhood) + ", " +
                        str(guests) + " guests, " +
                        "page " + str(section_offset))
            new_rooms = 0
            room_count = 0
            params = {}
            params["page"] = str(section_offset)
            params["source"] = "filter"
            params["location"] = self.search_area_name
            params["room_types[]"] = room_type
            params["neighborhoods[]"] = neighborhood
            response = airbnb_ws.ws_request_with_repeats(self.config,
                                                         self.config.URL_API_SEARCH_ROOT,
                                                         params)
            json_response = response.json()
            for result in json_response["results_json"]["search_results"]:
                room_id = int(result["listing"]["id"])
                if room_id is not None:
                    room_count += 1
                    listing = self.listing_from_search_page_json(result, room_id)
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
                neighborhood_id = self.get_neighborhood_id(neighborhood)
                self.log_progress(room_type, neighborhood_id,
                                  guests, section_offset, has_rooms)
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

    def get_neighborhood_id(self, neighborhood):
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
            conn = self.config.connect()
            cur = conn.cursor()
            cur.execute(sql, (self.survey_id, neighborhood,))
            neighborhood_id = cur.fetchone()[0]
            cur.close()
            conn.commit()
            cur = conn.cursor()
            cur.execute(sql, (self.survey_id, neighborhood,))
            neighborhood_id = cur.fetchone()[0]
            cur.close()
            conn.commit()
            return neighborhood_id
        except psycopg2.Error:
            raise
        except Exception:
            return None

    def get_neighborhoods_from_search_area(self):
        try:
            conn = self.config.connect()
            cur = conn.cursor()
            cur.execute("""
                select name
                from neighborhood
                where search_area_id =  %s
                order by name""", (self.search_area_id,))
            neighborhoods = []
            while True:
                row = cur.fetchone()
                if row is None:
                    break
                neighborhoods.append(row[0])
            cur.close()
            return neighborhoods
        except Exception:
            logger.error("Failed to retrieve neighborhoods from %s",
                         self.search_area_id)
            raise



class ABSurveyByZipcode(ABSurvey):
    """
    Subclass of Survey that carries out a survey by looping over
    zipcodes as defined in a separate table
    """

    def search(self, flag):
        logger.info("=" * 70)
        logger.info("Survey {survey_id}, for {search_area_name}".format(
            survey_id=self.survey_id, search_area_name=self.search_area_name
        ))
        ABSurvey.update_survey_entry(self, self.config.SEARCH_BY_ZIPCODE)
        logger.info("Searching by zipcode")
        zipcodes = self.get_zipcodes_from_search_area()
        for room_type in self.room_types:
            try:
                for zipcode in zipcodes:
                    self.__search_zipcode(str(zipcode), room_type, self.survey_id,
                                          flag, self.search_area_name)
            except Exception:
                raise
        self.fini()

    def __search_zipcode(self, zipcode, room_type, survey_id,
                         flag, search_area_name):
        try:
            if room_type in ("Private room", "Shared room"):
                max_guests = min(4, self.config.SEARCH_MAX_GUESTS)
            else:
                max_guests = self.config.SEARCH_MAX_GUESTS
            for guests in range(1, max_guests):
                logger.debug("Searching for %(g)i guests", {"g": guests})
                for section_offset in range(0, self.config.SEARCH_MAX_PAGES):
                    if flag != self.config.FLAGS_PRINT:
                        # this efficiency check can be implemented later
                        count = self.page_has_been_retrieved(
                            room_type, str(zipcode),
                            guests, section_offset, self.config.SEARCH_BY_ZIPCODE)
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
                    room_count = self.get_search_page_info_zipcode(
                        room_type, zipcode, guests, section_offset, flag)
                    if flag == self.config.FLAGS_PRINT:
                        # for FLAGS_PRINT, fetch one page and print it
                        sys.exit(0)
                    if room_count < self.config.SEARCH_LISTINGS_ON_FULL_PAGE:
                        logger.debug("Final page of listings for this search")
                        break
        except Exception:
            raise

    def get_zipcodes_from_search_area(self):
        try:
            conn = self.config.connect()
            cur = conn.cursor()
            # Query from the manually-prepared zipcode table
            cur.execute("""
            select zipcode
            from zipcode z, search_area sa
            where sa.search_area_id = %s
            and z.search_area_id = sa.search_area_id
            """, (self.search_area_id,))
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
                        str(self.search_area_id))
            raise

    def get_search_page_info_zipcode(self, room_type,
                                     zipcode, guests, section_offset, flag):
        try:
            logger.info("-" * 70)
            logger.info(room_type + ", zipcode " + str(zipcode) + ", " +
                        str(guests) + " guests, " + "page " +
                        str(section_offset + 1))
            room_count = 0
            new_rooms = 0
            params = {}
            params["guests"] = str(guests)
            params["section_offset"] = str(section_offset)
            params["source"] = "filter"
            params["location"] = zipcode
            params["room_types[]"] = room_type
            response = airbnb_ws.ws_request_with_repeats(self.config,
                                                         self.config.URL_API_SEARCH_ROOT,
                                                         params)
            json_response = response.json()
            for result in json_response["results_json"]["search_results"]:
                room_id = int(result["listing"]["id"])
                if room_id is not None:
                    room_count += 1
                    listing = self.listing_from_search_page_json(result, room_id)
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
                self.log_progress(room_type, zipcode,
                                  guests, section_offset, has_rooms)
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


def ABSurveyGlobal(ABSurvey):
    """
    Special search to randomly choose rooms from a range rather than to
    look at specific areas of the world.
    """

    def search(self, flag, search_by):
        logger.info("-" * 70)
        logger.info("Survey {survey_id}, for {search_area_name}".format(
            survey_id=self.survey_id, search_area_name=self.search_area_name
        ))
        ABSurvey.update_survey_entry(self, self.config.SEARCH_AREA_GLOBAL)
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
        self.fini()

