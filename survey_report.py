#!/usr/bin/python
""" 
Run reports on a log file from a survey
"""
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
    log_file_object=open(filename)
    dt_objects = []
    dt_diffs = []

    connection_error_count = 0
    total_connection_error_count = 0
    node_results_list = []
    page_and_zoom_complete = 0
    # Collect data from lines in the log
    # 2018-02-07 05:17:02,836 INFO    Searching rectangle: Shared room, guests = 2, prices in [60, 80], zoom factor = 1
    # 2018-02-07 05:17:04,114 INFO    Page 01 returned 01 listings
    # 2018-02-07 05:17:04,114 INFO    Results:  1 pages, 20 new rooms
    p_page = re.compile(r"([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}),([0-9]{3}) INFO    Page ([0-9]{2}) returned ([0-9]+) listings")
    p_rectangle = re.compile(r"Searching rectangle:\s+zoom factor = ([0-9]+), node\s*=\s*(.*)")
    p_result = re.compile(r"Results:\s+([0-9]+) pages, ([0-9]+) new rooms")
    p_survey = re.compile(r"Survey\s+([0-9]+), for (.*)")
    p_max_zoom = re.compile("Searching by bounding box, max_zoom=([0-9]+)")

    # Read raw data from the log file
    firstline = log_file_object.readline()
    p_date = re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}")
    match = p_date.search(firstline)
    survey_start_date = match.group(0)
    zoom = 0
    max_zoom = 0

    for line in log_file_object:
        # Response time raw data for page requests
        if "Page" in line:
            match = p_page.match(line)
            dt_string = match.group(1)
            dt_objects.append([datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S"),
                               connection_error_count])
            connection_error_count = 0
            if zoom == max_zoom and int(match.group(3)) == 20 and int(match.group(4)) == 18:
                page_and_zoom_complete += 1
        # Node details (at beginning of a node searchk
        elif "Searching rectangle:" in line:
            match = p_rectangle.search(line)
            if match:
                zoom = int(match.group(1))
                node = match.group(2)
        elif "Results: " in line:
        # Results of a node search: record the zoom from the beginning
            match = p_result.search(line)
            if match:
                pages = int(match.group(1))
                new_rooms = int(match.group(2))
                # Add an item to the new rooms list for this node
                node_results_list.append(
                    {"zoom": zoom, "node": node,
                     "pages": pages, "new_rooms": new_rooms}
                )
        # Request error
        elif "WARNING" in line and "connectionError" in line:
            total_connection_error_count += 1
        # For complete log files (one run, beginning to end), these items are in the header
        # Put them at the end of the ifs as they are rarely encountered
        elif "Survey " in line:
            match = p_survey.search(line)
            search_area = match.group(2)
        elif "max_zoom" in line:
            match = p_max_zoom.search(line)
            max_zoom = int(match.group(1))

    #------------------------------------------------------------------------
    # Calculations
    #------------------------------------------------------------------------
    # Response time calculations
    dt_previous = dt_objects[0][0]
    for dt_object in dt_objects[1:]:
        dt_diffs.append([min((dt_object[0] - dt_previous).seconds, 100),
                         dt_object[1]])
        dt_previous = dt_object[0]

    dt_buckets = [0] * 101
    total_response_time = 0
    for dt_diff in dt_diffs:
        dt_buckets[dt_diff[0]] += 1
        total_response_time += dt_diff[0]
    mean_response_time = float(total_response_time)/float(len(dt_diffs))

    # Connection error calculations
    connection_error_buckets  = [0] * 100
    for dt_diff in dt_diffs:
        connection_error_buckets[dt_diff[1]] += 1

    # Pages per listing efficiency calculations
    total_listing_count = 0
    zoom_level_new_rooms = [0] * (max_zoom + 1)
    for node_results in node_results_list:
        try:
            zoom_level_new_rooms[node_results["zoom"]] += node_results["new_rooms"]
            total_listing_count += node_results["new_rooms"]
        except:
            pass

    total_request_count = 0
    zoom_level_page_requests = [0] * (max_zoom + 1)
    for node_results in node_results_list:
        try:
            zoom_level_page_requests[node_results["zoom"]] += node_results["pages"]
            total_request_count += node_results["pages"]
        except:
            pass

    # Not used right now: print details of response time and connection error distributions
    if details:
        print("Request time buckets")
        for index, val in enumerate(dt_buckets):
            if val > 0:
                print(index, val)
        print("Connection error buckets")
        for index, val in enumerate(connection_error_buckets):
            if val > 0:
                print(index, val)

    #------------------------------------------------------------------------
    # PRINT REPORT
    #------------------------------------------------------------------------
    # Header
    print("")
    print("*" * 80)
    print("{}\tSearch results for {}, survey {} on {}{}".format(
        printColor.BOLD,
        search_area, survey_id, survey_start_date, printColor.END))
    print("")

    print("=" * 80)
    print("\t{}New rooms, by zoom level{}".format(printColor.BOLD, printColor.END))
    print("" * 80)
    print("\tZoom level\tNew rooms")
    print("-" * 80)
    for zoom_level in range(max_zoom):
        # Items goes over zoom?
        print("\t{}\t\t{}".format(zoom_level, zoom_level_new_rooms[zoom_level]))
    print("")

    # Print out visited pages
    print("=" * 80)
    print("\t{}Requests (pages visited), by zoom factor{}".format(printColor.BOLD, printColor.END))
    print("" * 80)
    print("\tZoom level\tPage requests")
    print("-" * 80)
    for zoom_level in range(max_zoom):
        print("\t{}\t\t{}".format(zoom_level, zoom_level_page_requests[zoom_level]))
    print("")

    # Print out summary
    print("=" * 80)
    print("\t{}Survey summary{}".format(printColor.BOLD, printColor.END))
    print("")
    print("\tTotal listing count\t{}".format(total_listing_count))
    print("\tTotal request count\t{}".format(total_request_count))
    print("\tRequests per listing\t{0:.2f}".format(float(total_request_count)/total_listing_count))
    print("\tMean response time\t{0:.1f} seconds".format(mean_response_time))
    print("\tTotal request time\t{0:.1f} hours"
          .format(total_request_count * mean_response_time / 3600.0))
    print("\tConnection error count\t{}".format(total_connection_error_count))
    if total_connection_error_count > 0:
        print("\tRequests per error\t{0:.0f}"
              .format(total_request_count / total_connection_error_count))
    else:
        print("\tRequests per error\tNaN")
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
