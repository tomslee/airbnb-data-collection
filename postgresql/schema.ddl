DROP TABLE IF EXISTS room;

CREATE TABLE room (
    room_id                        integer NOT NULL
   ,host_id                        integer NULL
   ,room_type                      varchar(255) NULL
   ,country                        varchar(255) NULL
   ,city                           varchar(255) NULL
   ,neighborhood                   varchar(255) NULL
   ,address                        varchar(1023) NULL
   ,reviews                        integer NULL
   ,overall_satisfaction           float NULL
   ,accommodates                   integer NULL
   ,bedrooms                       decimal(5,2) NULL
   ,bathrooms                      decimal(5,2) NULL
   ,price                          float NULL
   ,deleted                        integer NULL
   ,minstay                        integer NULL
   ,last_modified                  timestamp NULL DEFAULT current_timestamp
   ,latitude                       numeric(30,6) NULL
   ,longitude                      numeric(30,6) NULL
   ,survey_id                      integer NOT NULL DEFAULT 999999
   ,location                       geometry
   ,PRIMARY KEY (room_id ,survey_id ) 
)
;

COMMENT ON TABLE room IS 
	'The room_id is a unique ID on Airbnb. It may occur in several surveys (survey_id)'
;

DROP TABLE IF EXISTS search_area;

CREATE TABLE search_area (
    search_area_id                 SERIAL
   ,name                           varchar(255) NULL DEFAULT 'UNKNOWN'
   ,PRIMARY KEY (search_area_id) 
)
;

DROP TABLE IF EXISTS city;

CREATE TABLE city (
    city_id                        SERIAL
   ,name                           varchar(255) NULL
   ,search_area_id                 integer NULL
   ,PRIMARY KEY (city_id) 
)
;

DROP TABLE IF EXISTS neighborhood;

CREATE TABLE neighborhood (
    neighborhood_id                SERIAL
   ,name                           varchar(255) NULL
   ,search_area_id                 integer NULL
   ,PRIMARY KEY (neighborhood_id) 
)
;

DROP TABLE IF EXISTS survey;

CREATE TABLE survey (
    survey_id                      SERIAL
   ,survey_date                    date NULL DEFAULT current_date
   ,survey_description             varchar(255) NULL
   ,search_area_id                 integer NULL
   ,PRIMARY KEY (survey_id) 
)
;

COMMENT ON TABLE survey IS 
	'Each collection of rooms for a given city (search area) over a short period of time (usually a day) is called a survey. It''s a snapshot of the state of Airbnb listings in a particular place at a particular time.'
;

DROP TABLE IF EXISTS survey_search_page;

CREATE TABLE survey_search_page (
    survey_id                      integer NOT NULL
   ,room_type                      varchar(255) NOT NULL
   ,neighborhood_id                integer NULL
   ,page_number                    integer NOT NULL
   ,guests                         integer NOT NULL
   ,has_rooms                      smallint NULL
   ,page_id                        SERIAL
   ,PRIMARY KEY (page_id) 
)
;

COMMENT ON TABLE survey_search_page IS 
	'This table tracks search progress during the first stage of a survey, to avoid going through all the rooms again and again. Once a survey is complete, the rows could be deleted for that survey.'
;

DROP VIEW IF EXISTS host;

CREATE VIEW host(
  host_id, 
  city,
  survey_id,
  listings,
  reviews,
  relative_income
  )
AS
SELECT host_id, sa.name city, r.survey_id AS survey_id, 
  count(*) listings, 
  sum(reviews) reviews, 
  sum(price * reviews) relative_income 
FROM room r, survey s, search_area sa
WHERE r.survey_id = s.survey_id
AND s.search_area_id = sa.search_area_id
AND r.deleted = 0
GROUP BY sa.name, r.survey_id, host_id
