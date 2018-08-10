#! /usr/bin/python3
"""
Reverse geocoding
"""

import googlemaps
import argparse
import json
from airbnb_config import ABConfig
import sys
import logging

format_string = "%(asctime)-15s %(levelname)-8s%(message)s"
logging.basicConfig(level=logging.INFO, format=format_string)
logger = logging.getLogger()

# Suppress informational logging from requests module
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

class Location():

    def __init__(self, lat_round, lng_round):
        self.lat_round = lat_round
        self.lng_round = lng_round
        self.neighborhood = None
        self.sublocality = None
        self.locality = None
        self.level2 = None
        self.level1 = None
        self.country = None

    @classmethod
    def from_db(cls, lat_round, lng_round):
        return cls(lat_round, lng_round)



class BoundingBox():
    """
    Get max and min lat and long for a search area
    """

    def __init__(self, bounding_box):
        (self.bb_s_lat,
         self.bb_n_lat,
         self.bb_w_lng,
         self.bb_e_lng) = bounding_box

    @classmethod
    def from_db(cls, config, search_area):
        try:
            cls.search_area = search_area
            conn = config.connect()
            cur = conn.cursor()
            sql = """
            SELECT bb_s_lat, bb_n_lat, bb_w_lng, bb_e_lng
            FROM search_area
            WHERE name = %s
            """
            cur.execute(sql, (search_area,))
            # (self.bb_s_lat,
             # self.bb_n_lat,
             # self.bb_w_lng,
             # self.bb_e_lng) = cur.fetchone()
            bounding_box = cur.fetchone()
            cur.close()
            return cls(bounding_box)
        except:
            logger.exception("Exception in BoundingBox_from_db: exiting")
            sys.exit()

    @classmethod
    def from_google(cls, config, search_area):
        try:
            gmaps = googlemaps.Client(key=config.GOOGLE_API_KEY)
            results = gmaps.geocode((search_area))
            bounds = results[0]["geometry"]["bounds"]
            bounding_box = (bounds["southwest"]["lat"],
                            bounds["northeast"]["lat"],
                            bounds["southwest"]["lng"],
                            bounds["northeast"]["lng"],)
            return cls(bounding_box)
        except:
            logger.exception("Exception in BoundingBox_from_google: exiting")
            sys.exit()

    @classmethod
    def from_args(cls, config, args):
        try:
            bounding_box = (args.bb_s_lat, args.bb_n_lat,
                            args.bb_w_lng, args.bb_e_lng)
            return cls(bounding_box)
        except:
            logger.exception("Exception in BoundingBox_from_args: exiting")
            sys.exit()

def select_lat_lng(config, bounding_box):
    """
    Return a pair of lat_round and lng_round values from the Location table
    for which the country has not yet been set.
    """
    try:
        conn = config.connect()
        cur = conn.cursor()
        sql = """
        SELECT lat_round, lng_round
        FROM location
        WHERE country IS NULL
        AND lat_round BETWEEN %s AND %s
        AND lng_round BETWEEN %s AND %s
        LIMIT 1
        """
        args = (bounding_box.bb_s_lat,
                bounding_box.bb_n_lat,
                bounding_box.bb_w_lng,
                bounding_box.bb_e_lng)
        cur.execute(sql, args)
        try:
            (lat_round, lng_round) = cur.fetchone()
        except:
            # No more results
            return None
        cur.close()
        location = Location(lat_round, lng_round)
        return location
    except Exception: 
        logger.exception("Exception in select_lat_lng: exiting")
        sys.exit()


def update_location(config, location):
    """
    Insert or update a location with the required address information
    """
    try:
        conn = config.connect()
        cur = conn.cursor()
        sql = """
        UPDATE location
        SET neighborhood = %s,
        sublocality = %s,
        locality = %s,
        level2 = %s,
        level1 = %s,
        country = %s
        WHERE lat_round = %s AND lng_round = %s
        """
        update_args = ( location.neighborhood,
                location.sublocality,
                location.locality,
                location.level2,
                location.level1,
                location.country,
                location.lat_round,
                location.lng_round,
               )
        cur.execute(sql, update_args)
        cur.close()
        conn.commit()
        return True
    except:
        logger.exception("Exception in update_location")
        return False


def reverse_geocode(config, location):
    """ 
    Return address information from the Google API as a Location object for a given lat lng
    """
    gmaps = googlemaps.Client(key=config.GOOGLE_API_KEY)
    # Look up an address with reverse geocoding
    # lat = 41.782
    # lng = -72.693
    lat = location.lat_round
    lng = location.lng_round
    results = gmaps.reverse_geocode((lat, lng))

    # Parsing the result is described at
    # https://developers.google.com/maps/documentation/geocoding/web-service-best-practices#ParsingJSON

    json_file = open("geocode.json", mode="w", encoding="utf-8")
    json_file.write(json.dumps(results, indent=4, sort_keys=True))
    json_file.close()
    #  In practice, you may wish to only return the first result (results[0])

    for result in results:
        if (location.neighborhood and
                location.sublocality and
                location.locality and
                location.level2 and
                location.level1 and
                location.country):
            break
        address_components = result['address_components']
        for address_component in address_components:
            if (location.neighborhood is None
                    and "neighborhood" in address_component["types"]):
                location.neighborhood = address_component["long_name"]
            elif (location.sublocality is None
                  and "sublocality" in address_component["types"]):
                location.sublocality = address_component["long_name"]
            elif (location.locality is None
                  and "locality" in address_component["types"]):
                location.locality = address_component["long_name"]
            elif (location.level2 is None
                  and "administrative_area_level_2" in
                  address_component["types"]):
                location.level2 = address_component["long_name"]
            elif (location.level1 is None
                  and "administrative_area_level_1" in
                  address_component["types"]):
                location.level1 = address_component["long_name"]
            elif (location.country is None
                  and "country" in address_component["types"]):
                location.country = address_component["long_name"]
    return location


def main():
    """ Controlling routine that calls the others """
    config = ABConfig()
    parser = argparse.ArgumentParser(
        description='reverse geocode')
        # usage='%(prog)s [options]')
    # These arguments should be more carefully constructed. Right now there is
    # no defining what is required, and what is optional, and what contradicts
    # what.
    parser.add_argument("--sa",
                        metavar="search_area", type=str,
                        help="""search_area""")
    parser.add_argument("--lat",
                        metavar="lat", type=float,
                        help="""lat""")
    parser.add_argument("--lng",
                        metavar="lng", type=float,
                        help="""lng""")
    parser.add_argument("--bb_n_lat",
                        metavar="bb_n_lat", type=float,
                        help="""bb_n_lat""")
    parser.add_argument("--bb_s_lat",
                        metavar="bb_s_lat", type=float,
                        help="""bb_s_lat""")
    parser.add_argument("--bb_e_lng",
                        metavar="bb_e_lng", type=float,
                        help="""bb_e_lng""")
    parser.add_argument("--bb_w_lng",
                        metavar="bb_w_lng", type=float,
                        help="""bb_w_lng""")
    parser.add_argument("--count",
                        metavar="count", type=int,
                        help="""number_of_lookups""")
    args = parser.parse_args()
    search_area = args.sa
    if args.count:
        count = args.count
    else:
        count = 1000
    if search_area:
        # bb = BoundingBox.from_db(config, search_area)
        # print(bb.bb_s_lat, bb.bb_n_lat, bb.bb_w_lng, bb.bb_e_lng)
        bounding_box = BoundingBox.from_google(config, search_area)
        logger.info("Bounding box for %s = (%s, %s, %s, %s)",
                    search_area,
                    bounding_box.bb_s_lat, bounding_box.bb_n_lat,
                    bounding_box.bb_w_lng, bounding_box.bb_e_lng)
    if args.bb_n_lat:
        bounding_box = BoundingBox.from_args(config, args)
    if not count:
        sys.exit(0)
    for lookup in range(1, count):
        location = select_lat_lng(config, bounding_box)
        if location is None:
            logger.info("No more locations")
            sys.exit(0)
        location = reverse_geocode(config, location)
        logger.debug(
            "nbhd={}, subloc={}, loc={}, l2={}, l1={}, country={}."
            .format(
                location.neighborhood,
                location.sublocality,
                location.locality,
                location.level2,
                location.level1,
                location.country)
            )
        success = update_location(config, location)
        if success:
            logger.info("Update succeeded: %s, %s: %s of %s",
                        location.lat_round, location.lng_round,
                        lookup, count)
        else:
            logger.warn("Update failed: %s, %s: %s of %s",
                        location.lat_round, location.lng_round,
                        lookup, count)

if __name__ == "__main__":
    main()

