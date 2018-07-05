# Airbnb project notes

## Adding location information from Google maps

Information returned from the API is like this example:

> python reverse_geocode.py --lat=40.83 --lng=-73.90
neighbourhood=Morrisania,
sublocality=Bronx,
locality=New York,
level2=Bronx County,
level1=New York,
country=United States

The fields include "administrative_area_level_1" and
"administrative_area_level_2". Others are named as shown.

>   ALTER TABLE room ADD COLUMN lat_round NUMERIC(9,4), ADD COLUMN lng_round
NUMERIC(9,4)

>    CREATE TABLE LOCATION

## PostGIS queries

The room.location column is in SRID 4326.

To find the SRID from the project file, load the file to http://prj2epsg.org/search

Import the shapefile using PgAdminIII.

Create an index:

> 	CREATE INDEX gis_nyc_borough_gix ON gis_nyc_borough USING GIST (geom);

To set the SRID:

> 	select UpdateGeometrySRID('Schema Name', 'mytable', 'the_geom', newSRID)

After setting the SRID of the imported shapefile properly:

> 	select  count(*)
	from room r, gis_nyc_borough b
	where survey_id = 105
	and st_contains(b.geom,	st_transform(r.location, 2263)) = true;

And this:

>	select b.boroname as borough, count(*) listings, 
	sum(reviews) visits, 
	sum(reviews * price)/1000.0 relative_income
	from room r, gis_nyc_borough b
	where survey_id = 105
	and st_contains(b.geom, st_transform(r.location, 2263)) = true
	group by b.boroname
	order by 3 desc;


