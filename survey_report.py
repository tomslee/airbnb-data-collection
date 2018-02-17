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
    total_connection_error_count = 0
    new_rooms_list = []
    page_and_zoom_complete = 0
    # Collect data from lines in the log
    # 2018-02-07 05:17:02,836 INFO    Searching rectangle: Shared room, guests = 2, prices in [60, 80], zoom factor = 1
    # 2018-02-07 05:17:04,114 INFO    Page 01 returned 01 listings
    # 2018-02-07 05:17:04,114 INFO    Results:  1 pages, 0 new rooms, Shared room, 2 guests, prices in [60, 80]
    p_page = re.compile("([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}),([0-9]{3}) INFO    Page ([0-9]{2}) returned ([0-9]+) listings")
    p_rectangle = re.compile("Searching rectangle: ([^,]+), guests = ([0-9]+), prices in \[([0-9]+), ([0-9]+)\], zoom factor = ([0-9]+)")
    p_result = re.compile("Results:\s+([0-9]+) pages, ([0-9]+) new rooms, .+")
    p_survey = re.compile("Survey\s+([0-9]+), for (.*)")
    p_max_zoom = re.compile("Searching by bounding box, max_zoom=([0-9]+)")

    firstline = f.readline()
    p_date = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}")
    match = p_date.search(firstline)
    survey_start_date = match.group(0)

    for line in f:
        # Response time raw data for page requests
        if "Page" in line:
            match = p_page.match(line)
            dt_string = match.group(1)
            dt_objects.append([datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S"), connection_error_count])
            connection_error_count = 0
            if zoom==max_zoom and int(match.group(3))==20 and int(match.group(4))==18:
                page_and_zoom_complete += 1
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
        elif "WARNING" in line and "Network request exception" in line:
            total_connection_error_count += 1
        # For compete log files (one run, beginning to end), these items are in the header
        # Put them at the end of the ifs as they are rarely encountered
        elif "Survey " in line:
            match = p_survey.search(line)
            search_area = match.group(2)
        elif "max_zoom" in line:
            match = p_max_zoom.search(line)
            max_zoom = int(match.group(1))

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
    mean_response_time = float(total_response_time)/float(len(dt_diffs))

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
    print("*" * 80)
    print("{}\tSearch results for {}, survey {} on {}{}".format(printColor.BOLD, 
        search_area, survey_id, survey_start_date, printColor.END))
    print("")

    # Print out new rooms
    max_guests = 16
    total_listing_count = 0
    total_request_count = 0
    arr_eh = [[0] * (max_zoom + 1) for i in range(max_guests)]
    arr_pr = [[0] * (max_zoom + 1) for i in range(max_guests)]
    arr_sr = [[0] * (max_zoom + 1) for i in range(max_guests)]
    results = {"Entire home/apt": arr_eh,
        "Private room": arr_pr, 
        "Shared room":  arr_sr}
    for rectangle in new_rooms_list:
        # print("Rect: " + str(rectangle))
        try:
            results[rectangle["room_type"]][rectangle["guests"]][rectangle["zoom"]] += rectangle["new_rooms"]
            total_listing_count += rectangle["new_rooms"]
        except:
            pass
    print("=" * 80)
    print("\t{}New rooms, by room type, guests, and zoom factor{}".format(printColor.BOLD, printColor.END))
    print("" * 80)
    print("\tListing type\tGuests\tNew rooms")
    print("-" * 80)
    for k, v in results.items():
        # Items goes over room_type, k is the room type and v is a 2-d array
        for guests, zoom in enumerate(v):
            if zoom != [0] * (max_zoom + 1):
                print("\t{}\t{}\t{}".format( k, guests, zoom))
    print("")

    # Print out visited pages
    arr_eh = [[0] * (max_zoom + 1) for i in range(max_guests)]
    arr_pr = [[0] * (max_zoom + 1) for i in range(max_guests)]
    arr_sr = [[0] * (max_zoom + 1) for i in range(max_guests)]
    results = {"Entire home/apt": arr_eh,
        "Private room": arr_pr, 
        "Shared room":  arr_sr}
    for rectangle in new_rooms_list:
        # print("Rect: " + str(rectangle))
        try:
            results[rectangle["room_type"]][rectangle["guests"]][rectangle["zoom"]] += rectangle["pages"]
            total_request_count += rectangle["pages"]
        except:
            pass
    print("=" * 80)
    print("\t{}Requests (pages visited), by room type, guests, and zoom factor{}".format(printColor.BOLD, printColor.END))
    print("" * 80)
    print("\tListing type\tGuests\tRequests")
    print("-" * 80)
    for k, v in results.items():
        # Items goes over room_type, k is the room type and v is a 2-d array
        for guests, zoom in enumerate(v):
            if zoom != [0] * (max_zoom + 1):
                print("\t{}\t{}\t{}".format( k, guests, zoom))
    print("")

    # Print out summary
    print("=" * 80)
    print("\t{}Survey summary{}".format(printColor.BOLD, printColor.END))
    print("")
    print("\tTotal listing count\t{}".format(total_listing_count))
    print("\tTotal request count\t{}".format(total_request_count))
    print("\tMean response time\t{0:.1f} seconds".format(mean_response_time))
    print("\tTotal request time\t{0:.1f} hours".format(total_request_count * mean_response_time / 3600.0))
    print("\tConnection error count\t{}".format(total_connection_error_count))
    if total_connection_error_count > 0:
        print("\tRequests per error\t{}".format(total_request_count / total_connection_error_count))
    else:
        print("\tRequests per error\tNaN")
    print("\tRequests per listing\t{0:.2f}".format(float(total_request_count)/total_listing_count))
    print("\tExhausted searches\t{}".format(page_and_zoom_complete))
    print("")

    # Print footer
    print("*" * 80)
    print("")

try:
    survey_id = sys.argv[1]
except:
    survey_id = None

if survey_id is not None:
    runit(survey_id, False)
else:
    print("\n")
    print("Usage: python survey_report.py <survey_id>")
    print("where you have a log file survey-<survey_id>.log")
   
    

