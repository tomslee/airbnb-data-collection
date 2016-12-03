# Airbnb web site scraper

## Disclaimers

The script scrapes the Airbnb web site to collect data about the shape of the company's business. No guarantees are made about the quality of data obtained using this script, statistically or about an individual page. So please check your results.

Airbnb is increasingly making it difficult to scrape significant amounts of data from the site. I now have to run the script using a number of proxy IP addresses to avoid being turned away, and that costs money. I am afraid that I cannot help in finding or working with proxy IP services. If you would rather not make the attempt yourself, I will be happy to run collections for you when time allows.

## Using the script

You must be comfortable messing about with databases and python to use this.

The airbnb.py script works with a PostgreSQL database. You need to have the PostGIS extension installed. The schema is in the two files postgresql/schema.sql and postgresql/functions.sql. You need to run those to create the database tables to start with.

To run the airbnb.py scraper you will need to use python 3.4 and install the modules listed at the top of the file. The difficult one is lxml: you'll have to go to their web site to get it. It doesn't seem to be in the normal python repositories so if you're on Linux you may get it through an application package manager (apt-get or yum, for example).

Various parameters are stored in a configuration file, which is read in as \$USER.config. Make a copy of example.config and edit it to match your database and the other parameters. The script uses proxies, so if you don't want those you may have to edit out some part of the code.

### Upgrading the database schema

If you have moved from an earlier version of the script, you may need to 
update the schema of the room table by adding columns. To do this, run 

    python schema_update.py

### Preparing to run a survey

To check that you can connect to the database, run

    python airbnb.py -dbp

where python is python3.

Add a search area (city) to the database:

    python airbnb.py -asa "City Name"

This adds a city to the "search_area" table, and a set of neighborhoods to the "neighborhoods" table.

Add a survey description for that city:

    python airbnb.py -asv "City Name"

This makes an entry in the survey table, and should give you a survey_id value.

### Running a survey 

There are several kinds of survey supported. For some cities, Airbnb provides a list of "neighbourhoods", and one search loops over each neighbourhood in turn. If the city does not have neighbourhoods defined by Airbnb, this search will probably underestimate the number of listings by a large amount.

Run a neighbourhood-by-neighbourhood search:

    python airbnb.py -s survey_id

This can take a long time (hours). Like many sites, Airbnb turns away requests (HTTP error 503) if you make too many in a short time, so the script tries waiting regularly. If you have to stop in the middle, that's OK -- running it again picks up where it left off (after a bit of a pause).

The search collects room_id values from the Airbnb search pages for a city. The next step is to visit each room page and get the details.

To fill in the room details:

    python airbnb.py -f survey_id

Again, this can take a long time (days for big cities). But again, if you have to cancel in the middle it's not a big deal; just run the command again to pick up. You can even run multiple instances of the "-f" step at the same time to speed it up

To run a search by bounding box:

    python airbnb.py -sb survey_id

The bounding box search is the one I use for most cases (as of November 2016).
Thanks to contributions from Sam Kaufman it now saves information on the search step, and there is no need to run an "-f" step afterwards.

Search by bounding box does a recursive geographical search, breaking a bounding box that surrounds a city into smaller pieces, and continuing to search while new listings are identified. This currently relies on adding the bounding box to the search_area table manually. A bounding box for a city can be found by entering the city at the following page:

    http://www.mapdevelopers.com/geocode_bounding_box.php

Then you can update the search_are table with a statement like this:

```
UPDATE search_area
SET bb_n_lat = NN.NNN,
bb_s_lat = NN.NNN,
bb_e_lng = NN.NNN,
bb_w_lng = NN.NNN
WHERE search_area_id = NNN
```

Ideally I'd like to automate this process. I am still experimenting with a combination of search_max_pages and search_max_rectangle_zoom (in the user.config file) that picks up all the listings in a reasonably efficient manner. It seems that for a city, search_max_pages=20 and search_max_rectangle_zoom=6 works well.

- Search by zip code requires a set of zip codes for a city, stored in a separate table (which is not currently included). The table definition is 
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

# Results

The basic data is in the table "room". A complete search of a given city's listings is a "survey" and the surveys are tracked in table *survey*. If you want to see all the listings for a given survey, you can query the stored procedure survey_room (survey_id) from a tool such as PostgreSQL psql.

```
SELECT *
FROM room
WHERE deleted = 0
AND survey_id = NNN
```

I also create separate tables that have GIS shapefiles for cities in them, and create views that provide a more accurate picture of the listings in a city, but that work is outside the scope of this project.
