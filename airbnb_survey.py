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

logger = logging.getLogger()


class ABSurvey():

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
        logfile = "survey_{survey_id}.log".format(survey_id=self.survey_id)
        filelog_handler = logging.FileHandler(logfile, encoding="utf-8")
        filelog_formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s%(message)s')
        filelog_handler.setFormatter(filelog_formatter)
        # create a console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(config.log_level)
        ch_formatter = logging.Formatter('%(levelname)-8s%(message)s')
        console_handler.setFormatter(ch_formatter)

        # logging: set log file name, format, and level
        logger.addHandler(filelog_handler)
        logger.addHandler(console_handler)

        # Suppress informational logging from requests module
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logger.propagate = False

    def set_search_area(self):
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

    def listing_from_search_page_json(self, result, room_id, room_type):
        try:
            listing = ABListing(self.config, room_id, self.survey_id, room_type)
            # listing
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

    def log_progress(self, room_type, neighborhood_id,
                     guests, page_number, has_rooms):
        """ Add an entry to the survey_progress_log table to record the fact
        that a page has been visited.
        This does not apply to search by bounding box, but does apply to both
        neighborhood and zipcode searches, which is why it is in ABSurvey.
        """
        try:
            page_info = (self.survey_id, room_type, neighborhood_id,
                         guests, page_number, has_rooms)
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
        """ Wrap up a survey: correcting status and survey_date
        """
        try:
            logger.info("Finishing survey {survey_id}, for {search_area_name}".format(
                survey_id=self.survey_id, search_area_name=self.search_area_name
            ))
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
                    logger.debug("has_rooms = " + str(has_rooms) +
                                " for neighborhood " + neighborhood)
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
                logger.debug("has_rooms = " + str(has_rooms) +
                            " for zipcode " + str(zipcode))
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
        self.get_logged_progress()
        self.get_bounding_box()

    def get_logged_progress(self):
        try:
            sql = """
            select room_type, guests, price_min, price_max, quadtree_node
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
                logger.info("No progress logged for survey {}".format(self.survey_id))
                self.logged_progress = None
            else:
                logged_progress = {}
                logged_progress["room_type"] = row[0]
                logged_progress["guests"] = row[1]
                logged_progress["price_range"] = [row[2], row[3]]
                logged_progress["quadtree"] = eval(row[4])
                logger.info( """Retrieved logged progress: {rt}, {g} guests, price {pmin}-{pmax}""".
                format(rt = logged_progress["room_type"],
                    g=logged_progress["guests"], 
                    pmin=logged_progress["price_range"][0], 
                    pmax=logged_progress["price_range"][1]))
                logger.info("\tquadtree node {quadtree}"
                        .format(quadtree=repr(logged_progress["quadtree"])))
                self.logged_progress = logged_progress
        except Exception:
            logger.exception("Exception in get_progress: setting logged progress to None")
            self.logged_progress = None

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
            self.bounding_box = list(cur.fetchone())
            cur.close()
            # Validate the bounding box
            if None in self.bounding_box:
                logger.error("Invalid bounding box: contains 'None'")
                return
            if self.bounding_box[0] <= self.bounding_box[2]:
                logger.error("Invalid bounding box: n_lat must be > s_lat")
                return
            if self.bounding_box[1] <= self.bounding_box[3]:
                logger.error("Invalid bounding box: e_lng must be > w_lng")
                return
            logger.info("Bounding box: " + str(self.bounding_box))
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
            logger.info("Searching by bounding box, max_zoom={max_zoom}"
                    .format(max_zoom=self.config.SEARCH_MAX_RECTANGLE_ZOOM ))
            # Initialize search parameters
            # quadtree_node holds the quadtree: each rectangle is
            # divided into 00 | 01 | 10 | 11, and the next level down adds
            # on another rectangle.
            price_increments = [0, 40, 60, 80, 100, 120,
                                140, 180, 200, 300, 500,
                                700, 1000, 1500, 50000]
            max_price = {"Private room": 500,
                         "Entire home/apt": 100000,
                         "Shared room": 500}
            # set starting point 
            guests_start = 1
            quadtree_node = []
            room_types_start_index = 0
            price_start_index = 0
            # set starting point (for survey being resumed)
            if self.logged_progress is not None:
                room_types_start_index = self.room_types.index(self.logged_progress["room_type"])
                guests_start = self.logged_progress["guests"]
                price_start_index = price_increments.index(self.logged_progress["price_range"][0])
                logger.info("""Restarting survey {survey_id} at room_type={room_type}, guests={guests}, price={price}
                        quadtree_node={quadtree_node}"""
                        .format(survey_id=self.survey_id, room_type=self.room_types[room_types_start_index],
                            guests=guests_start, price=price_increments[price_start_index],
                            quadtree_node = quadtree_node))
            # Starting point set: now loop
            for room_type in self.room_types[room_types_start_index:]:
                if room_type in ("Private room", "Shared room"):
                    max_guests = 4
                else:
                    max_guests = self.config.SEARCH_MAX_GUESTS
                for guests in range(guests_start, max_guests):
                    for i in range(price_start_index, len(price_increments) - 1):
                        price_range = [price_increments[i], price_increments[i+1]]
                        if price_range[1] > max_price[room_type]:
                            continue
                        progress = {}
                        progress["room_type"] = room_type
                        progress["guests"] = guests
                        progress["price_range"] = price_range
                        progress["quadtree"] = quadtree_node
                        self.recurse_quadtree(
                            room_type, guests, price_range, quadtree_node, flag)
                        # reset starting price
                        price_start_index = 0
                # reset the starting point so that (in the event of a resumed
                # survey) the next room type gets all guest counts.
                guests_start = 1
        except Exception:
            logger.exception("Error")
        finally:
            ABSurvey.fini(self)

    def recurse_quadtree(self, room_type, guests, price_range, quadtree_node, flag):
        """
        Recursive function to search for listings inside a rectangle.
        The actual search calls are done in search_node, and
        this method prints output and sets up new rectangles, if necessary,
        for another round of searching.

        To match Airbnb's use of SW and NE corners, quadrants are divided
        like this:

                     [0,1] (NW)   |   [0,0] (NE)
                     ---------------------------
                     [1,1] (SW)   |   [1,0] (SE)
        
        The quadrants are searched in the order [0,0], [0,1], [1,0], [1,1]
        """
        try:
            if self.subtree_previously_completed(quadtree_node):
                # go to the next subtree 
                # (I'm sure there is a more elegant way to do this)
                if quadtree_node[-1] == [0,0]:
                    quadtree_node[-1] = [0,1]
                elif quadtree_node[-1] == [0,1]:
                    quadtree_node[-1] = [1,0]
                elif quadtree_node[-1] == [1,0]:
                    quadtree_node[-1] = [1,1]
                elif quadtree_node[-1] == [1,1]:
                    del quadtree_node[-1]
                return

            # Only search this node if it has not been previously searched
            if (self.logged_progress is None or
                len(quadtree_node) >= len(self.logged_progress["quadtree"])):
                (new_rooms, page_count) = self.search_node(room_type, guests, price_range,
                                                           quadtree_node, flag)
                # we are off and searching: set logged_progress to None so 
                # future guests, prices etc don't get truncated
                self.logged_progress = None
            else:
                logger.debug("Node previously searched: {quadtree}".format(quadtree=quadtree_node))
                # if the logged_progress has more depth, recurse
                if len(self.logged_progress["quadtree"]) >= len(quadtree_node):
                    page_count = self.config.SEARCH_MAX_PAGES
            # The max zoom is set in config, but decrease it by one for each guest
            # so that high guest counts don't zoom in (which turns out to generate
            # very few new rooms but take a lot of time)
            zoomable = len(quadtree_node) < max(1, (self.config.SEARCH_MAX_RECTANGLE_ZOOM - 2 * (guests - 1)))
            # zoomable = len(quadtree_node) < self.config.SEARCH_MAX_RECTANGLE_ZOOM
            # If (new_rooms > 0 or page_count == self.config.SEARCH_MAX_PAGES) and zoomable:
            # zoom in if the search returned a full set of SEARCH_MAX_PAGES pages even 
            # if no rooms were new, as there may still be new rooms that show up at 
            # higher zoom levels.
            if page_count == self.config.SEARCH_MAX_PAGES and zoomable:
                # append a node to the quadtree for a new level
                quadtree_node.append([0,0])
                new_rooms = self.recurse_quadtree(room_type, guests, price_range,
                                                    quadtree_node, flag)
                # next quadrant
                quadtree_node[-1] = [0,1]
                new_rooms = self.recurse_quadtree(room_type, guests, price_range,
                                                    quadtree_node, flag)
                # next quadrant
                quadtree_node[-1] = [1,0]
                new_rooms = self.recurse_quadtree(room_type, guests, price_range,
                                                    quadtree_node, flag)
                # next quadrant
                quadtree_node[-1] = [1,1]
                new_rooms = self.recurse_quadtree(room_type, guests, price_range,
                                                    quadtree_node, flag)
                # the search of the quadtree below this node is complete: 
                # remove the leaf element from the tree and return to go up a level
                del quadtree_node[-1]
            logger.debug("Returning from recurse_quadtree for {}".format(quadtree_node))
            if flag == self.config.FLAGS_PRINT:
                # for FLAGS_PRINT, fetch one page and print it
                sys.exit(0)
        except TypeError as te:
            logger.exception("TypeError in recurse_quadtree")
            logger.error(te.args)
            raise
        except:
            logger.exception("Error in recurse_quadtree")
            raise

    def search_node(self, room_type, guests, price_range, quadtree_node, flag):
        """
            rectangle is (n_lat, e_lng, s_lat, w_lng)
            returns number of *new* rooms and number of pages tested
        """
        try:
            rectangle = self.get_rectangle_from_quadtree_node(quadtree_node)
            logger.info("-" * 70)
            logger.info("Searching rectangle: {room_type}, {guests} guests, prices in [{p1}, {p2}], "
                .format(room_type=room_type, guests=str(guests),
                        p1=str(price_range[0]), p2=str(price_range[1])))
            logger.info("\tquadtree_node = {quadtree_node}".format(quadtree_node=str(quadtree_node)))
            logger.debug("Rectangle: N={n:+.5f}, E={e:+.5f}, S={s:+.5f}, W={w:+.5f}".format(
                n=rectangle[0], e=rectangle[1], s=rectangle[2], w=rectangle[3])
            )
            new_rooms = 0
            room_total = 0
            for page_number in range(1, self.config.SEARCH_MAX_PAGES + 1):
                room_count = 0
                # set up the parameters for the request
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
                # make the http request
                response = airbnb_ws.ws_request_with_repeats(self.config, self.config.URL_API_SEARCH_ROOT, params)
                # process the response
                json = response.json()
                for result in json["results_json"]["search_results"]:
                    room_id = int(result["listing"]["id"])
                    if room_id is not None:
                        room_count += 1
                        room_total += 1
                        listing = self.listing_from_search_page_json(result, room_id, room_type)
                        if listing is None:
                            continue
                        if listing.host_id is not None:
                            listing.deleted = 0
                            if flag == self.config.FLAGS_ADD:
                                if listing.save(self.config.FLAGS_INSERT_NO_REPLACE):
                                    new_rooms += 1
                            elif flag == self.config.FLAGS_PRINT:
                                print(room_type, listing.room_id)
                # Log page-level results
                logger.info("Page {page_number:02d} returned {room_count:02d} listings"
                        .format(page_number=page_number, room_count=room_count))
                if flag == self.config.FLAGS_PRINT:
                    # for FLAGS_PRINT, fetch one page and print it
                    sys.exit(0)
                if room_count < self.config.SEARCH_LISTINGS_ON_FULL_PAGE:
                    # If a full page of listings is not returned by Airbnb,
                    # this branch of the search is complete.
                    logger.debug("Final page of listings for this search")
                    break
            # Log rectangle-level results
            logger.info(("Results: {new_rooms} new rooms from {page_count} pages "
                "for {room_type}, {g} guests, prices in [{p1}, {p2}]").format(
                             room_type=room_type, g=str(guests),
                             p1=str(price_range[0]),
                             p2=str(price_range[1]),
                             new_rooms=str(new_rooms),
                             page_count=str(page_number)))
            logger.debug("\tquadtree_node = {quadtree_node}".format(quadtree_node=str(quadtree_node)))
            # log progress
            self.log_progress(room_type, guests, price_range[0], price_range[1], quadtree_node)
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

    def get_rectangle_from_quadtree_node(self, quadtree_node):
        try:
            rectangle = self.bounding_box
            for node in quadtree_node:
                [n_lat, w_lng, s_lat, e_lng] = rectangle
                blur = abs(n_lat - s_lat) * self.config.SEARCH_RECTANGLE_EDGE_BLUR
                # find the mindpoints of the rectangle
                mid_lat = (n_lat + s_lat)/2.0
                mid_lng = (e_lng + w_lng)/2.0
                # overlap quadrants to ensure coverage at high zoom levels
                # Airbnb max zoom (18) is about 0.004 on a side.
                rectangle = []
                if node==[0,0]: # NE
                    rectangle = [n_lat + blur, e_lng + blur, mid_lat - blur, mid_lng - blur]
                elif node==[0,1]: # NW
                    rectangle = [n_lat + blur, mid_lng + blur, mid_lat - blur, w_lng - blur]
                elif node==[1,0]: # SE
                    rectangle = [mid_lat + blur, e_lng + blur, s_lat - blur, mid_lng - blur]
                elif node==[1,1]: # SW
                    rectangle = [mid_lat + blur, mid_lng + blur, s_lat - blur, w_lng - blur]
            return rectangle
        except:
            logger.exception("Exception in get_rectangle_from_quadtree_node")
            return None

    def subtree_previously_completed(self, quadtree_node):
        # Return if the child subtree of this node was completed
        # in a previous survey
        subtree_previously_completed = False
        if len(quadtree_node) > 0 and self.logged_progress["quadtree"] is not None:
            s_this_quadrant = ''.join(str(quadtree_node[i][j]) 
                    for j in range(0,2)
                    for i in range(0,len(quadtree_node)))
            s_logged_progress = ''.join(str(self.logged_progress["quadtree"][i][j]) 
                    for j in range(0,2)
                    for i in range(0,len(quadtree_node)))
            if int(s_this_quadrant) < int(s_logged_progress):
                subtree_previously_completed = True
                logger.debug("Subtree previously completed: {quadtree}".format(quadtree=quadtree_node))
        return subtree_previously_completed


    def log_progress(self, room_type, guests, price_min, price_max, quadtree_node):
        try:
            # This upsert statement requires PostgreSQL 9.5
            # Convert the quadrant to a string with repr() before storing it
            sql = """
            insert into survey_progress_log_bb 
            (survey_id, room_type, guests, price_min, price_max, quadtree_node)
            values 
            (%s, %s, %s, %s, %s, %s)
            on conflict ON CONSTRAINT survey_progress_log_bb_pkey
            do update
                set room_type = %s
                , guests = %s
                , price_min = %s
                , price_max = %s
                , quadtree_node = %s
                , last_modified = now()
            where survey_progress_log_bb.survey_id = %s
            """
            conn = self.config.connect()
            cur = conn.cursor()
            cur.execute(sql, (self.survey_id, room_type, 
                guests, price_min, price_max, repr(quadtree_node),
                room_type, guests, price_min, price_max, repr(quadtree_node),
                self.survey_id))
            cur.close()
            conn.commit()
            logger.debug("Progress logged")
            return True
        except Exception:
            logger.warning("""Progress not logged: survey not affected, but
                    resume will not be available if survey is truncated.""")
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
            self.__global_search()
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
                max_guests = 4
            else:
                max_guests = self.config.SEARCH_MAX_GUESTS
            for guests in range(1, max_guests):
                logger.debug("Searching for %(g)i guests", {"g": guests})
                for page_number in range(1, self.config.SEARCH_MAX_PAGES + 1):
                    if flag != self.config.FLAGS_PRINT:
                        count = self.page_has_been_retrieved(
                            room_type, neighborhood, guests, page_number,
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
                    logger.info(("{room_type} ({g} guests): neighborhood {neighborhood}: "
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
            logger.info(room_type + ", " +
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
                    listing = self.listing_from_search_page_json(result, room_id, room_type)
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
            logger.error("Failed to retrieve neighborhoods from " +
                        str(search_area_id))
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
                i = 0
                for zipcode in zipcodes:
                    i += 1
                    self.__search_zipcode(str(zipcode), room_type, self.survey_id,
                                        flag, self.search_area_name)
            except Exception:
                raise
        self.fini()

    def __search_zipcode(self, zipcode, room_type, survey_id,
                         flag, search_area_name):
        try:
            if room_type in ("Private room", "Shared room"):
                max_guests = 4
            else:
                max_guests = self.config.SEARCH_MAX_GUESTS
            for guests in range(1, max_guests):
                logger.debug("Searching for %(g)i guests", {"g": guests})
                for page_number in range(1, self.config.SEARCH_MAX_PAGES + 1):
                    if flag != self.config.FLAGS_PRINT:
                        # this efficiency check can be implemented later
                        count = self.page_has_been_retrieved(
                            room_type, str(zipcode),
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
                    room_count = self.get_search_page_info_zipcode(
                        room_type, zipcode, guests, page_number, flag)
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
            response = airbnb_ws.ws_request_with_repeats(self.config, self.config.URL_API_SEARCH_ROOT, params)
            json = response.json()
            for result in json["results_json"]["search_results"]:
                room_id = int(result["listing"]["id"])
                if room_id is not None:
                    room_count += 1
                    listing = self.listing_from_search_page_json(result, room_id, room_type)
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
