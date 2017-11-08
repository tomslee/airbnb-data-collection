--updated schema_current by moving sequences in front of tables and adding db-level settings from schema.sql

CREATE EXTENSION postgis;

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

CREATE SEQUENCE city_city_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


CREATE SEQUENCE neighborhood_neighborhood_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE search_area_search_area_id_seq
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;

CREATE SEQUENCE survey_survey_id_seq
            START WITH 1
            INCREMENT BY 1
            NO MINVALUE
            NO MAXVALUE
            CACHE 1;

CREATE SEQUENCE survey_search_page_page_id_seq
                START WITH 1
                INCREMENT BY 1
                NO MINVALUE
                NO MAXVALUE
                CACHE 1;

CREATE TABLE public.city
(
  city_id integer NOT NULL DEFAULT nextval('city_city_id_seq'::regclass),
  name character varying(255),
  search_area_id integer,
  CONSTRAINT city_pkey PRIMARY KEY (city_id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE public.neighborhood
(
  neighborhood_id integer NOT NULL DEFAULT nextval('neighborhood_neighborhood_id_seq'::regclass),
  name character varying(255),
  search_area_id integer,
  CONSTRAINT neighborhood_pkey PRIMARY KEY (neighborhood_id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE public.room
(
  room_id integer NOT NULL,
  host_id integer,
  room_type character varying(255),
  country character varying(255),
  city character varying(255),
  neighborhood character varying(255),
  address character varying(1023),
  reviews integer,
  overall_satisfaction double precision,
  accommodates integer,
  bedrooms numeric(5,2),
  bathrooms numeric(5,2),
  price double precision,
  deleted integer,
  minstay integer,
  last_modified timestamp without time zone DEFAULT now(),
  latitude numeric(30,6),
  longitude numeric(30,6),
  survey_id integer NOT NULL DEFAULT 999999,
  location geometry,
  coworker_hosted integer,
  extra_host_languages character varying(255),
  name character varying(255),
  property_type character varying(255),
  currency character varying(20),
  rate_type character varying(20),
  CONSTRAINT room_pkey PRIMARY KEY (room_id, survey_id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE public.schema_version
(
  version numeric(5,2) NOT NULL,
  CONSTRAINT schema_version_pkey PRIMARY KEY (version)
)
WITH (
  OIDS=FALSE
);


CREATE TABLE public.search_area
(
  search_area_id integer NOT NULL DEFAULT nextval('search_area_search_area_id_seq'::regclass),
  name character varying(255) DEFAULT 'UNKNOWN'::character varying,
  abbreviation character varying(10), -- Short form for city: used in views, in particular.
  bb_n_lat double precision,
  bb_e_lng double precision,
  bb_s_lat double precision,
  bb_w_lng double precision,
  CONSTRAINT search_area_pkey PRIMARY KEY (search_area_id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE public.survey
(
  survey_id integer NOT NULL DEFAULT nextval('survey_survey_id_seq'::regclass),
  survey_date date DEFAULT ('now'::text)::date,
  survey_description character varying(255),
  search_area_id integer,
  comment character varying(255),
  survey_method character varying(20) DEFAULT 'neighborhood'::character varying, -- neighborhood or zipcode
  status smallint,
  CONSTRAINT survey_pkey PRIMARY KEY (survey_id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE public.survey_progress_log
(
  survey_id integer NOT NULL,
  room_type character varying(255) NOT NULL,
  neighborhood_id integer,
  page_number integer NOT NULL,
  guests integer NOT NULL,
  page_id integer NOT NULL DEFAULT nextval('survey_search_page_page_id_seq'::regclass),
  has_rooms smallint,
  zoomstack character varying(255),
  CONSTRAINT survey_search_page_pkey PRIMARY KEY (page_id)
)
WITH (
  OIDS=FALSE
);



CREATE TABLE public.survey_progress_log_bb
(
  survey_id integer NOT NULL,
  room_type character varying(255),
  guests integer,
  price_min double precision,
  price_max double precision,
  quadtree_node character varying(1024),
  last_modified timestamp without time zone DEFAULT now(),
  median_node text,
  CONSTRAINT survey_progress_log_bb_pkey PRIMARY KEY (survey_id)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE public.zipcode
(
  zipcode character varying(10) NOT NULL,
  search_area_id integer,
  CONSTRAINT z PRIMARY KEY (zipcode),
  CONSTRAINT zipcode_search_area_id_fkey FOREIGN KEY (search_area_id)
      REFERENCES public.search_area (search_area_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);

CREATE TABLE public.zipcode_us
(
  zip integer NOT NULL,
  type character varying(20),
  primary_city character varying(50),
  acceptable_cities character varying(300),
  state character varying(3),
  county character varying(50),
  timezone character varying(50),
  area_code character varying(50),
  latitude numeric(6,2),
  longitude numeric(6,2),
  world_region character varying(10),
  country character varying(20),
  decommissioned integer,
  estimated_population integer,
  notes character varying(255),
  CONSTRAINT pk_zipcode PRIMARY KEY (zip)
)
WITH (
  OIDS=FALSE
);

CREATE OR REPLACE FUNCTION public.trg_location()
  RETURNS trigger AS
$BODY$
BEGIN
  NEW.location := st_setsrid(
	st_makepoint(NEW.longitude, NEW.latitude),
	4326
	);
	RETURN NEW;
 END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

CREATE TRIGGER trg_location 
BEFORE INSERT OR UPDATE 
OF latitude, longitude ON room 
FOR EACH ROW 
    EXECUTE PROCEDURE trg_location();

ALTER SEQUENCE neighborhood_neighborhood_id_seq OWNED BY neighborhood.neighborhood_id;
ALTER SEQUENCE survey_search_page_page_id_seq OWNED BY survey_progress_log.page_id;
ALTER SEQUENCE survey_survey_id_seq OWNED BY survey.survey_id;
ALTER SEQUENCE search_area_search_area_id_seq OWNED BY search_area.search_area_id;
ALTER SEQUENCE city_city_id_seq OWNED BY city.city_id;
