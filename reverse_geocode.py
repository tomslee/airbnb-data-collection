#! /usr/bin/python3
"""
Reverse geocoding
"""

import googlemaps
import argparse
import json
from airbnb_config import ABConfig

class Location():

    def __init__(self):
        self.neighborhood = None
        self.sublocality = None
        self.locality = None
        self.level2 = None
        self.level1 = None
        self.country = None

def main():
    """ Reverse geocode a lat, lng argumet """
    parser = argparse.ArgumentParser(
        description='reverse geocode')
        # usage='%(prog)s [options]')
    parser.add_argument("--lat",
                        metavar="lat", type=float,
                        help="""lat""")
    parser.add_argument("--lng",
                        metavar="lng", type=float,
                        help="""lng""")
    args = parser.parse_args()
    lat = args.lat
    lng = args.lng
    # return address information for lat lng
    config = ABConfig()
    gmaps = googlemaps.Client(key=config.GOOGLE_API_KEY)
    # Look up an address with reverse geocoding
    # lat = 41.782
    # lng = -72.693
    results = gmaps.reverse_geocode((lat, lng))

    # Parsing the result is described at
    # https://developers.google.com/maps/documentation/geocoding/web-service-best-practices#ParsingJSON

    json_file = open("geocode.json", mode="w", encoding="utf-8")
    json_file.write(json.dumps(results, indent=4, sort_keys=True))
    json_file.close()
    #  In practice, you may wish to only return the first result (results[0])

    location = Location()
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
    print("neighbourhood={}, sublocality={}, locality={}, level2={}, level1={}, country={}"
          .format(
              location.neighborhood,
              location.sublocality,
              location.locality,
              location.level2,
              location.level1,
              location.country)
         )

if __name__ == "__main__":
    main()

