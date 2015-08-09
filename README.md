Airbnb web site scraper
=======================

Disclaimer
----------

The script scrapes the Airbnb web site to collect data about the shape of
the company's business. No guarantees are made about the quality of data
obtained using this script, statistically or about an individual page. So 
please check your results.

Using the script
----------------
You must be comfortable messing about with databases and python to use this.

The airbnb.py script works with a PostgreSQL database. The schema is in the two
files postgresql/schema.ddl and postgresql/functions.sql. You need to run those
to create the database to start with.

To run it you will need to use python 3.4 and install the modules listed at the
top of the file. The difficult one is lxml: you'll have to go to their web site
to get it. It doesn't seem to be in the normal python repositories so if you're
on Linux you may get it through an application package manager (apt-get or yum,
for example).

Various parameters are stored in a configuration file, which is read in as
$USER.config. Make a copy of example.config and edit it to match your database
and the other parameters. The script uses proxies, so if you don't want those
you may have to edit out some part of the code.

To check that you can connect to the database, run
  : python airbnb.py -dbp
where python is python3.

Add a search area (city) to the database.
  : python airbnb.py -asa "City Name"

Add a survey description for that city.
  : python airbnb.py -asv "City Name"

This should give you a survey_id value.

Run a search:
  : python airbnb.py -s survey_id

This can take a long time (hours). Like many sites, Airbnb turns away requests
(HTTP error 503) if you make too many in a short time, so the script tries
waiting regularly. If you have to stop in the middle, that's OK -- running it
again picks up where it left off (after a bit of a pause).

The search collects room_id values from the Airbnb search pages for a city. The next step is to visit each room page and get the details.

To fill in the room details:
  : python airbnb.py -f

Again, this can take a long time (days for big cities) because of the need to
pause between requests. But again, if you have to cancel in the middle it's not
a big deal; just run the command again to pick up.

The basic data is in the table "room". A complete search of a given city's
listings is a "survey" and the surveys are tracked in table *survey*. If you
want to see all the listings for a given survey, you can query the stored
procedure survey_room (survey_id) from a tool such as PostgreSQL psql.

  : SELECT * from survey_room (survey_id)

If you query directly from the room table, note that some rooms will have
deleted = 1. Omit these from your queries.
