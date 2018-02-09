#!/usr/bin/python
from datetime import datetime
import re
import sys

class printColor:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

def runit(survey_id, details):
    filename = "survey-{}.log".format(survey_id)
    f=open(filename)
    dt_objects = []
    dt_diffs = []

    connection_error_count = 0
    new_rooms_list = []
    # Collect data from lines in the log
    # 2018-02-07 05:17:02,836 INFO    Searching rectangle: Shared room, guests = 2, prices in [60, 80], zoom factor = 1
    # 2018-02-07 05:17:04,114 INFO    Page 01 returned 01 listings
    # 2018-02-07 05:17:04,114 INFO    Results:  1 pages, 0 new rooms, Shared room, 2 guests, prices in [60, 80]
    p_rectangle = re.compile("Searching rectangle: ([^,]+), guests = ([0-9]+), prices in \[([0-9]+), ([0-9]+)\], zoom factor = ([0-9]+)")
    p_result = re.compile("Results:\s+([0-9]+) pages, ([0-9]+) new rooms, .+")
    p_survey = re.compile("Survey\s+([0-9]+), for (.*)")
    firstline = f.readline()
    p_date = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}")
    match = p_date.search(firstline)
    survey_start_date = match.group(0)
    for line in f:
        # Response time raw data for page requests
        if "returned" in line:
            dt_string = line[:23].replace(",","")
            dt_string = dt_string[:len(dt_string)-3]
            dt_objects.append([datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S"), connection_error_count])
            connection_error_count = 0
        # Rectangle details
        elif "Searching rectangle:" in line:
            match = p_rectangle.search(line)
            if match is not None:
                room_type = match.group(1)
                guests = int(match.group(2))
                price_min = int(match.group(3))
                price_max = int(match.group(4))
                zoom = int(match.group(5))
        # Results of a rectangle
        elif "Results:  " in line:
            match = p_result.search(line)
            if match is not None:
                pages = int(match.group(1))
                new_rooms = int(match.group(2))
                new_rooms_list.append({"room_type": room_type,
                            "guests": guests,
                            "price_min": price_min,
                            "price_max": price_max,
                            "zoom": zoom,
                            "pages": pages,
                            "new_rooms": new_rooms})
        # Request error
        elif "connectionError" in line:
            connection_error_count += 1
        elif "Survey " in line:
            match = p_survey.search(line)
            search_area = match.group(2)
            

    # Response time calculations
    dt_previous = dt_objects[0][0]
    for dt in dt_objects[1:]:
        dt_diffs.append([min((dt[0] - dt_previous).seconds, 100), dt[1]])
        dt_previous = dt[0]

    dt_buckets = [0] * 101
    total_response_time = 0
    for t in dt_diffs:
        dt_buckets[t[0]] += 1
        total_response_time += t[0]
    mean_response_time = total_response_time/len(dt_diffs)

    # Connection error calculations
    connection_error_buckets  = [0] * 100
    for t in dt_diffs:
        connection_error_buckets[t[1]] += 1

    # Not used right now: print details of response time and connection error distributions
    if details:
        print("Request time buckets")
        for ix, val in enumerate(dt_buckets):
            if val > 0:
                print(ix, val)
        print("Connection error buckets")
        for ix, val in enumerate(connection_error_buckets):
            if val > 0:
                print(ix, val)

    #------------------------------------------------------------------------
    # PRINT REPORT
    #------------------------------------------------------------------------
    # Header
    print("")
    print("{}\tSearch results for {}, survey {} on {}{}".format(printColor.GREEN, 
        search_area, survey_id, survey_start_date, printColor.END))
    print("")

    # Print out new rooms
    max_guests = 16
    max_zoom = 7
    total_listing_count = 0
    total_request_count = 0
    arr_eh = [[0] * max_zoom for i in range(max_guests)]
    arr_pr = [[0] * max_zoom for i in range(max_guests)]
    arr_sr = [[0] * max_zoom for i in range(max_guests)]
    results = {"Entire home/apt": arr_eh,
        "Private room": arr_pr, 
        "Shared room":  arr_sr}
    for rectangle in new_rooms_list:
        # print("Rect: " + str(rectangle))
        results[rectangle["room_type"]][rectangle["guests"]][rectangle["zoom"]] += rectangle["new_rooms"]
        total_listing_count += rectangle["new_rooms"]
    print("=" * 80)
    print("\t{}New rooms, by room type, guests, and zoom factor{}".format(printColor.YELLOW, printColor.END))
    print("" * 80)
    print("\tListing type\tGuests\tNew rooms")
    print("-" * 80)
    for k, v in results.items():
        # Items goes over room_type, k is the room type and v is a 2-d array
        for guests, zoom in enumerate(v):
            if zoom != [0] * max_zoom:
                print("\t{}\t{}\t{}".format( k, guests, zoom))
    print("")

    # Print out visited pages
    max_guests = 16
    max_zoom = 7
    arr_eh = [[0] * max_zoom for i in range(max_guests)]
    arr_pr = [[0] * max_zoom for i in range(max_guests)]
    arr_sr = [[0] * max_zoom for i in range(max_guests)]
    results = {"Entire home/apt": arr_eh,
        "Private room": arr_pr, 
        "Shared room":  arr_sr}
    for rectangle in new_rooms_list:
        # print("Rect: " + str(rectangle))
        results[rectangle["room_type"]][rectangle["guests"]][rectangle["zoom"]] += rectangle["pages"]
        total_request_count += rectangle["pages"]
    print("=" * 80)
    print("\t{}Requests (pages visited), by room type, guests, and zoom factor{}".format(printColor.YELLOW, printColor.END))
    print("" * 80)
    print("\tListing type\tGuests\tPages visited")
    print("-" * 80)
    for k, v in results.items():
        # Items goes over room_type, k is the room type and v is a 2-d array
        for guests, zoom in enumerate(v):
            if zoom != [0] * max_zoom:
                print("\t{}\t{}\t{}".format( k, guests, zoom))
    print("")

    # Print out summary
    print("=" * 80)
    print("\t{}Survey summary{}".format(printColor.YELLOW, printColor.END))
    print("")
    print("\tTotal listing count\t{} (may not match database!)".format(total_listing_count))
    print("\tTotal request count\t{}".format(total_request_count))
    print("\tMean response time\t{} seconds".format(mean_response_time))
    print("\tTotal request time\t{0:.1f} hours".format(total_request_count * mean_response_time / 3600.0))
    print("\tRequests per listing\t{0:.2f}".format(float(total_request_count)/total_listing_count))
    print("")
    print("=" * 80)

try:
    survey_id = sys.argv[1]
except:
    survey_id = None

if survey_id is not None:
    runit(survey_id, False)
else:
    for survey_id in sorted(range(1760,2000), reverse=True):
        try:
            runit(survey_id, False)
        except:
            pass
   
    

