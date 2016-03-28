m Airbnb web site scraper
=======================

Disclaimers
-----------

The script scrapes the Airbnb web site to collect data about the shape of the company's business. No guarantees are made about the quality of data obtained using this script, statistically or about an individual page. So please check your results. Airbnb is increasingly making it difficult to scrape significant amounts of data from the site. I have to run the script using a number of proxy IP addresses to avoid being turned away, and that costs money. I am afraid that I cannot help in finding or working with proxy IP services. If you would rather not make the attempt yourself, I will be happy to run collections for you when time allows. 

Using the script
----------------

You must be comfortable messing about with databases and python to use this.

The airbnb.py script works with a PostgreSQL database. The schema is in the two files postgresql/schema.sql and postgresql/functions.sql. You need to run those to create the database tables to start with.

To run the airbnb.py scraper you will need to use python 3.4 and install the modules listed at the top of the file. The difficult one is lxml: you'll have to go to their web site to get it. It doesn't seem to be in the normal python repositories so if you're on Linux you may get it through an application package manager (apt-get or yum, for example).

Various parameters are stored in a configuration file, which is read in as \$USER.config. Make a copy of example.config and edit it to match your database and the other parameters. The script uses proxies, so if you don't want those you may have to edit out some part of the code.

To check that you can connect to the database, run

    : python airbnb.py -dbp

where python is python3.

Add a search area (city) to the database:

    : python airbnb.py -asa "City Name"

This adds a city to the "search_area" table, and a set of neighborhoods to the "neighborhoods" table.

Add a survey description for that city:

    : python airbnb.py -asv "City Name"

This makes an entry in the survey table, and should give you a survey_id value.

Run a search:

    : python airbnb.py -s survey_id

This can take a long time (hours). Like many sites, Airbnb turns away requests (HTTP error 503) if you make too many in a short time, so the script tries waiting regularly. If you have to stop in the middle, that's OK -- running it again picks up where it left off (after a bit of a pause).

The search collects room_id values from the Airbnb search pages for a city. The next step is to visit each room page and get the details.

To fill in the room details:

    : python airbnb.py -f survey_id

Again, this can take a long time (days for big cities). But again, if you have to cancel in the middle it's not a big deal; just run the command again to pick up.

Alternative search methods
--------------------------

The default search method (-s) loops over room_type, number of guests, and neighborhoods, looping over a number of pages for each combination until no more listings are found. This method has been found to identify almost all listings in most cities.

For some cities and for other regions of interest, looping over neighborhoods does not work well. If Airbnb has not identified neighborhoods for a city, many listings can be missed. For this reason, two other search methods are included.

- Search by zip code requires a set of zip codes for a city, stored in a separate table (which is not currently included).
- Search by bounding box does a recursive geographical search, breaking a bounding box that surrounds a city into smaller pieces, and continuing to search while new listings are identified. This currently relies on adding the bounding box to the search_area table manually. A bounding box for a city can be found by entering the city at the following page:

    : http://www.mapdevelopers.com/geocode_bounding_box.php

Ideally I'd like to automate this process. I am still experimenting with a combination of search_max_pages and search_max_rectangle_zoom that picks up all the listings in a reasonably efficient manner.

Results
-------

The basic data is in the table "room". A complete search of a given city's listings is a "survey" and the surveys are tracked in table *survey*. If you want to see all the listings for a given survey, you can query the stored procedure survey_room (survey_id) from a tool such as PostgreSQL psql.

    : SELECT * from survey_room (survey_id)

If you query directly from the room table, note that some rooms will have deleted = 1 and some may have deleted is NULL. You should only include rooms that have deleted = 0 in any queries you do.

I also create separate tables that have GIS shapefiles for cities in them, and create views that provide a more accurate picture of the listings in a city, but that work is outside the scope of this project.
