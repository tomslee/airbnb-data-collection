# Airbnb web site scraper

*I am no longer maintaining this script.*

I have been unable to maintain this script in a reasonable state for some time now, but I know there are people out there using it. If anybody wants to take over maintenance / ownership, I'd be very happy to help make the transition as easy as possible.

## Disclaimers

The script scrapes the Airbnb web site to collect data about the shape of the company's business. No guarantees are made about the quality of data obtained using this script, statistically or about an individual page. So please check your results.

Sometimes the Airbnb site refuses repeated requests. I run the script using a number of proxy IP addresses to avoid being turned away, and that costs money. I am afraid that I cannot help in finding or working with proxy IP services.

## Status and recent changes

### July 2019 (3.6)

There are continued problems getting the script to work with new AirBnB page layouts. The change
that affects this script most is reflected in airbnb_survey.py, around line 620. The items_offset values get incremented with each new page of a search, and it used to be that section_offset would change too. If you are having problems and know how to track down request URL's in a browser, do try different settings for these.

### May 2019 (3.6)

For several months this script has not been working properly, which is an indication of its likely future state. One of the changes to Airbnb's site design led to a failure to paginate properly through the listings of each query, so that additional listings would be added only very slowly. This problem is now solved.

### June 2018 (3.4)

As of April 2018, searches of the Airbnb web site only return listings with available booking dates in the near future (I do not know the precise criterion). In some cases, this leads to a 20% or 20% reduction in the number of listings obtained in a search area compared with earlier results.

In other words, there are listings on the Airbnb web site that do not get returned in searches. These are listings for which all the dates in their calendar are marked as unavailable.

### April 2018 (3.3)

After further changes to the Airbnb web site here is a new version, posted on April 29 2018. 

For this version to work, you need to find a key value from the Airbnb web site and fill in the api_key and url_api_search_root values in the configuration file. See example.config for more information. I should emphasize that I do not know what the api_key signifies or communicates to Airbnb.

The script still seems to miss a percentage of the listings in high-density areas (see comment in previous April 2018 entry).

### April 2018

A second change to the Airbnb web site in February 2018 broke the script again. After several weeks, I have uploaded a fixed version on April 8, 2018. The specific change has been addressed (listings on a search site were broken into two distinct sets, and I had been picking up only one), and running on several cities suggests that the script is working again, although there is an open question whether it is missing about 10% of listings.

I have also made a change to loop only over rectangles of increasingly small size, removing separate loops over number of guests, room_type, and price. This seems to increase efficiency considerably, with no loss of accuracy (if listings are missing -- see the previous paragraph -- I believe it is a separate issue).

It continues to be the case that only "python airbnb.py -sb <survey-number>" works as a search method. See below for instructions on how to set up such a survey.

### February 2018

For some time in January 2018 this script was not working at all, as Airbnb had changed the site layout. As of February 1 2018, tests on four cities are consistent with results from throughout 2017 for the "-sb" bounding-box survey, and I believe it can be used reliably in that way.

The "-sb" search that is all I do now is more efficient now. Set search_max_guests to 1 and search_do_loop_over_prices to 1, and the search does not doo separate loops over guests and price ranges. Instead, set a larger search_max_zoom (eg 12) as by covering all guests and price ranges at once, the search may need to zoom down to smaller rectangles.

## Prerequisites

- Python 3.4 or later
- PostgreSQL 9.5 or later (as the script uses "INSERT ... ON CONFLICT UPDATE")

## Using the script

You must be comfortable messing about with databases and python to use this. 
For running the script with docker please check: [Run Airbnb data collection with Docker](docker/README.md)

To run the airbnb.py scraper you will need to use python 3.4 or later and install the modules listed at the top of the file. The difficult one is lxml: you'll have to go to their web site to get it. It doesn't seem to be in the normal python repositories so if you're on Linux you may get it through an application package manager (apt-get or yum, for example). The Anaconda distribution includes lxml and many other packages, and that's now the one 
I use.

Various parameters are stored in a configuration file, which is read in as `$USER.config`. Make a copy of `example.config` and edit it to match your database and the other parameters. The script uses proxies, so if you don't want those you may have to edit out some part of the code.

If you want to run multiple concurrent surveys with different configuration parameters, you can do so by making a copy of your `user.config` file, editing it and running the airbnb.py scripts (see below) with an additional command line parameter. The database connection test would become

    python airbnb.py -dbp -c other.config

This was implemented initially to run bounding-box surveys for countries (maximum zoom of 8) and cities (maximum zoom of 6) at the same time.

### Installing and upgrading the database schema

The airbnb.py script works with a PostgreSQL database. You need to have the PostGIS extension installed. The schema is in the file `postgresql/schema_current.sql`. You need to run that file to create the database tables to start with (assuming both your user and database are named `airbnb`).


For example, if you use psql:

    psql --user airbnb airbnb < postgresql/schema_current.sql

### Preparing to run a survey

To check that you can connect to the database, run

    python airbnb.py -dbp

where python is python3.

Add a search area (city) to the database:

    python airbnb.py -asa "City Name"

This adds a city to the `search_area` table. It used to add a set of
neighbourhoods to the neighborhoods table, but as only "-sb" searches
are now supported that no longer happens.

Add a survey description for that city:

    python airbnb.py -asv "City Name"

This makes an entry in the `survey` table, and should give you a `survey_id` value.

### Running a survey 

There are three ways to run surveys:
- by neighbourhood
- by bounding box
- by zipcode

Of these, the bounding box is the one I use most and so is most thoroughly tested. The neighbourhood one is the easiest to set up, so you may want to try that first, but be warned that if Airbnb has not assigned neighbourhoods to the city you are searching, the results can be very incomplete.

For users of earlier releases: Thanks to contributions from Sam Kaufman the searches now save information on the search step, and there is no need to run an `-f` step after running a `-s` or `-sb` or `-sz` search: the information about each room is collected from the search pages.

#### Neighbourhood search

For some cities, Airbnb provides a list of "neighbourhoods", and one search loops over each neighbourhood in turn. If the city does not have neighbourhoods defined by Airbnb, this search will probably underestimate the number of listings by a large amount.

Run a neighbourhood-by-neighbourhood search:

    python airbnb.py -s survey_id

This can take a long time (hours). Like many sites, Airbnb turns away requests (HTTP error 503) if you make too many in a short time, so the script tries waiting regularly. If you have to stop in the middle, that's OK -- running it again picks up where it left off (after a bit of a pause).

#### Zipcode search

To run a search by zipcode (see below for setup):

    python airbnb.py -sz zipcode

Search by zip code requires a set of zip codes for a city, stored in a separate table (which is not currently included). The table definition is 
as follows:

```
CREATE TABLE zipcode (
  zipcode character varying(10) NOT NULL,
  search_area_id integer,
  CONSTRAINT z PRIMARY KEY (zipcode),
  CONSTRAINT zipcode_search_area_id_fkey 
    FOREIGN KEY (search_area_id) 
    REFERENCES search_area (search_area_id)
)
```

#### Bounding box search

To run a search by bounding box:

    python airbnb.py -sb survey_id

Search by bounding box does a recursive geographical search, breaking a bounding box that surrounds a city into smaller pieces, and continuing to search while new listings are identified. This currently relies on adding the bounding box to the search_area table manually. A bounding box for a city can be found by entering the city at the following page:

    http://www.mapdevelopers.com/geocode_bounding_box.php

Then you can update the `search_area` table with a statement like this:

```
UPDATE search_area
SET bb_n_lat = NN.NNN,
bb_s_lat = NN.NNN,
bb_e_lng = NN.NNN,
bb_w_lng = NN.NNN
WHERE search_area_id = NNN
```

Ideally I'd like to automate this process. I am still experimenting with a combination of search_max_pages and search_max_rectangle_zoom (in the user.config file) that picks up all the listings in a reasonably efficient manner. It seems that for a city, search_max_pages=20 and search_max_rectangle_zoom=6 works well.


## Results

The basic data is in the table `room`. A complete search of a given city's listings is a "survey" and the surveys are tracked in table `survey`. If you want to see all the listings for a given survey, you can query the stored procedure survey_room (survey_id) from a tool such as PostgreSQL psql.

```
SELECT *
FROM room
WHERE deleted = 0
AND survey_id = NNN
```

I also create separate tables that have GIS shapefiles for cities in them, and create views that provide a more accurate picture of the listings in a city, but that work is outside the scope of this project.
