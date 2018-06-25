#! /usr/bin/python3
"""
Reverse geocoding
"""

import googlemaps
import argparse
import json
from airbnb_config import ABConfig

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
    neighborhood = None
    sublocality = None
    locality = None
    lev2 = None
    region = None
    country = None
    for result in results:
        if (neighborhood and sublocality and locality and lev2 and region and
            country):
            break
        address_components = result['address_components']
        for address_component in address_components:
            if (neighborhood is None 
                and "neighborhood" in address_component["types"]):
                neighborhood = address_component["long_name"]
            elif (sublocality is None
                and "sublocality" in address_component["types"]):
                sublocality = address_component["long_name"]
            elif (locality is None
                  and "locality" in address_component["types"]):
                locality = address_component["long_name"]
            elif (lev2 is None
                  and "administrative_area_level_2" in
                  address_component["types"]):
                lev2 = address_component["long_name"]
            elif (region is None
                  and "administrative_area_level_1" in
                  address_component["types"]):
                region = address_component["long_name"]
            elif (country is None
                    and "country" in address_component["types"]):
                country = address_component["long_name"]
    
    print("nbr={}, sub={}, loc={}, lev2={}, reg={}, country={}".format(
        neighborhood, sublocality, locality, lev2, region, country))

if __name__ == "__main__":
    main()

