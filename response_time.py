#!/usr/bin/python
from datetime import datetime
import sys


def runit(f, x, details):
    dt_objects = []
    dt_diffs = []

    connection_error_count = 0
    for line in f:
        if "connectionError" in line:
            connection_error_count += 1
        if "returned" in line:
            dt_string = line[:23].replace(",","")
            dt_string = dt_string[:len(dt_string)-3]
            dt_objects.append([datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S"), connection_error_count])
            connection_error_count = 0

    dt_previous = dt_objects[0][0]
    for dt in dt_objects[1:]:
        dt_diffs.append([min((dt[0] - dt_previous).seconds, 100), dt[1]])
        dt_previous = dt[0]

    dt_buckets = [0] * 101
    total_response_time = 0
    for t in dt_diffs:
        dt_buckets[t[0]] += 1
        total_response_time += t[0]

    connection_error_buckets  = [0] * 100
    for t in dt_diffs:
        connection_error_buckets[t[1]] += 1

    if details:
        print("Request time buckets")
        for ix, val in enumerate(dt_buckets):
            if val > 0:
                print(ix, val)
        print("Connection error buckets")
        for ix, val in enumerate(connection_error_buckets):
            if val > 0:
                print(ix, val)

    print("Average response time for survey-{} = {} seconds".format(x, total_response_time/len(dt_diffs)))

try:
    survey_id = sys.argv[1]
except:
    survey_id = None

if survey_id is not None:
    filename = "survey-{}.log".format(survey_id)
    f=open(filename)
    runit(f, filename, True)
else:
    for x in sorted(range(1760,2000), reverse=True):
        filename = "survey_{}.log".format(x)
        try:
            f=open(filename)
            runit(f, x, False)
        except:
            pass
   
    

