--
-- PostgreSQL database dump
--

-- Dumped from database version 9.4.1
-- Dumped by pg_dump version 9.4.4
-- Started on 2016-03-25 10:22:39

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 192 (class 1259 OID 18018)
-- Name: city; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE city (
    city_id integer NOT NULL,
    name character varying(255),
    search_area_id integer
);


--
-- TOC entry 191 (class 1259 OID 18016)
-- Name: city_city_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE city_city_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 4633 (class 0 OID 0)
-- Dependencies: 191
-- Name: city_city_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE city_city_id_seq OWNED BY city.city_id;


--
-- TOC entry 196 (class 1259 OID 18034)
-- Name: survey; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE survey (
    survey_id integer NOT NULL,
    survey_date date DEFAULT ('now'::text)::date,
    survey_description character varying(255),
    search_area_id integer,
    comment character varying(255),
    survey_method character varying(20) DEFAULT 'neighborhood'::character varying,
    status smallint
);


--
-- TOC entry 4635 (class 0 OID 0)
-- Dependencies: 196
-- Name: TABLE survey; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE survey IS 'Each collection of rooms for a given city (search area) over a short period of time (usually a day) is called a survey. It''s a snapshot of the state of Airbnb listings in a particular place at a particular time.';


--
-- TOC entry 4636 (class 0 OID 0)
-- Dependencies: 196
-- Name: COLUMN survey.survey_method; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN survey.survey_method IS 'neighborhood or zipcode';


--
-- TOC entry 188 (class 1259 OID 17997)
-- Name: room; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE room (
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
    survey_id integer DEFAULT 999999 NOT NULL,
    location geometry
);


--
-- TOC entry 4638 (class 0 OID 0)
-- Dependencies: 188
-- Name: TABLE room; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE room IS 'The room_id is a unique ID on Airbnb. It may occur in several surveys (survey_id)';


--
-- TOC entry 194 (class 1259 OID 18026)
-- Name: neighborhood; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE neighborhood (
    neighborhood_id integer NOT NULL,
    name character varying(255),
    search_area_id integer
);


--
-- TOC entry 193 (class 1259 OID 18024)
-- Name: neighborhood_neighborhood_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE neighborhood_neighborhood_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 4641 (class 0 OID 0)
-- Dependencies: 193
-- Name: neighborhood_neighborhood_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE neighborhood_neighborhood_id_seq OWNED BY neighborhood.neighborhood_id;


--
-- TOC entry 198 (class 1259 OID 18043)
-- Name: survey_progress_log; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE survey_progress_log (
    survey_id integer NOT NULL,
    room_type character varying(255) NOT NULL,
    neighborhood_id integer,
    page_number integer NOT NULL,
    guests integer NOT NULL,
    page_id integer NOT NULL,
    has_rooms smallint
);


--
-- TOC entry 4643 (class 0 OID 0)
-- Dependencies: 198
-- Name: TABLE survey_progress_log; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE survey_progress_log IS 'This table tracks search progress during the first stage of a survey, to avoid going through all the rooms again and again. Once a survey is complete, the rows could be deleted for that survey.';


--
-- TOC entry 197 (class 1259 OID 18041)
-- Name: survey_search_page_page_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE survey_search_page_page_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 4645 (class 0 OID 0)
-- Dependencies: 197
-- Name: survey_search_page_page_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE survey_search_page_page_id_seq OWNED BY survey_progress_log.page_id;


--
-- TOC entry 195 (class 1259 OID 18032)
-- Name: survey_survey_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE survey_survey_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 4647 (class 0 OID 0)
-- Dependencies: 195
-- Name: survey_survey_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE survey_survey_id_seq OWNED BY survey.survey_id;


--
-- TOC entry 286 (class 1259 OID 20572)
-- Name: zipcode; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE zipcode (
    zipcode character varying(10) NOT NULL,
    search_area_id integer
);


--
-- TOC entry 4449 (class 2604 OID 18021)
-- Name: city_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY city ALTER COLUMN city_id SET DEFAULT nextval('city_city_id_seq'::regclass);


--
-- TOC entry 4450 (class 2604 OID 18029)
-- Name: neighborhood_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY neighborhood ALTER COLUMN neighborhood_id SET DEFAULT nextval('neighborhood_neighborhood_id_seq'::regclass);


--
-- TOC entry 4451 (class 2604 OID 18037)
-- Name: survey_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY survey ALTER COLUMN survey_id SET DEFAULT nextval('survey_survey_id_seq'::regclass);


--
-- TOC entry 4454 (class 2604 OID 18046)
-- Name: page_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY survey_progress_log ALTER COLUMN page_id SET DEFAULT nextval('survey_search_page_page_id_seq'::regclass);


--
-- TOC entry 4459 (class 2606 OID 18023)
-- Name: city_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY city
    ADD CONSTRAINT city_pkey PRIMARY KEY (city_id);


--
-- TOC entry 4461 (class 2606 OID 18031)
-- Name: neighborhood_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY neighborhood
    ADD CONSTRAINT neighborhood_pkey PRIMARY KEY (neighborhood_id);


--
-- TOC entry 4457 (class 2606 OID 18006)
-- Name: room_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY room
    ADD CONSTRAINT room_pkey PRIMARY KEY (room_id, survey_id);


--
-- TOC entry 4463 (class 2606 OID 18040)
-- Name: survey_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY survey
    ADD CONSTRAINT survey_pkey PRIMARY KEY (survey_id);


--
-- TOC entry 4465 (class 2606 OID 18048)
-- Name: survey_search_page_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY survey_progress_log
    ADD CONSTRAINT survey_search_page_pkey PRIMARY KEY (page_id);


--
-- TOC entry 4467 (class 2606 OID 20576)
-- Name: z; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY zipcode
    ADD CONSTRAINT z PRIMARY KEY (zipcode);


--
-- TOC entry 4455 (class 1259 OID 22687)
-- Name: ix_survey; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX ix_survey ON room USING btree (survey_id);


--
-- TOC entry 4469 (class 2620 OID 18116)
-- Name: trg_location; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_location BEFORE INSERT OR UPDATE OF latitude, longitude ON room FOR EACH ROW EXECUTE PROCEDURE trg_location();


--
-- TOC entry 4468 (class 2606 OID 20577)
-- Name: zipcode_search_area_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY zipcode
    ADD CONSTRAINT zipcode_search_area_id_fkey FOREIGN KEY (search_area_id) REFERENCES search_area(search_area_id);


--
-- TOC entry 4632 (class 0 OID 0)
-- Dependencies: 192
-- Name: city; Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON TABLE city FROM PUBLIC;
REVOKE ALL ON TABLE city FROM postgres;
GRANT ALL ON TABLE city TO postgres;
GRANT SELECT ON TABLE city TO reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE city TO editor;


--
-- TOC entry 4634 (class 0 OID 0)
-- Dependencies: 191
-- Name: city_city_id_seq; Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON SEQUENCE city_city_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE city_city_id_seq FROM postgres;
GRANT ALL ON SEQUENCE city_city_id_seq TO postgres;
GRANT SELECT,USAGE ON SEQUENCE city_city_id_seq TO editor;


--
-- TOC entry 4637 (class 0 OID 0)
-- Dependencies: 196
-- Name: survey; Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON TABLE survey FROM PUBLIC;
REVOKE ALL ON TABLE survey FROM postgres;
GRANT ALL ON TABLE survey TO postgres;
GRANT SELECT ON TABLE survey TO reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE survey TO editor;


--
-- TOC entry 4639 (class 0 OID 0)
-- Dependencies: 188
-- Name: room; Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON TABLE room FROM PUBLIC;
REVOKE ALL ON TABLE room FROM postgres;
GRANT ALL ON TABLE room TO postgres;
GRANT SELECT ON TABLE room TO reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE room TO editor;


--
-- TOC entry 4640 (class 0 OID 0)
-- Dependencies: 194
-- Name: neighborhood; Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON TABLE neighborhood FROM PUBLIC;
REVOKE ALL ON TABLE neighborhood FROM postgres;
GRANT ALL ON TABLE neighborhood TO postgres;
GRANT SELECT ON TABLE neighborhood TO reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE neighborhood TO editor;


--
-- TOC entry 4642 (class 0 OID 0)
-- Dependencies: 193
-- Name: neighborhood_neighborhood_id_seq; Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON SEQUENCE neighborhood_neighborhood_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE neighborhood_neighborhood_id_seq FROM postgres;
GRANT ALL ON SEQUENCE neighborhood_neighborhood_id_seq TO postgres;
GRANT SELECT,USAGE ON SEQUENCE neighborhood_neighborhood_id_seq TO editor;


--
-- TOC entry 4644 (class 0 OID 0)
-- Dependencies: 198
-- Name: survey_progress_log; Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON TABLE survey_progress_log FROM PUBLIC;
REVOKE ALL ON TABLE survey_progress_log FROM postgres;
GRANT ALL ON TABLE survey_progress_log TO postgres;
GRANT SELECT ON TABLE survey_progress_log TO reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE survey_progress_log TO editor;


--
-- TOC entry 4646 (class 0 OID 0)
-- Dependencies: 197
-- Name: survey_search_page_page_id_seq; Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON SEQUENCE survey_search_page_page_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE survey_search_page_page_id_seq FROM postgres;
GRANT ALL ON SEQUENCE survey_search_page_page_id_seq TO postgres;
GRANT SELECT,USAGE ON SEQUENCE survey_search_page_page_id_seq TO editor;


--
-- TOC entry 4648 (class 0 OID 0)
-- Dependencies: 195
-- Name: survey_survey_id_seq; Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON SEQUENCE survey_survey_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE survey_survey_id_seq FROM postgres;
GRANT ALL ON SEQUENCE survey_survey_id_seq TO postgres;
GRANT SELECT,USAGE ON SEQUENCE survey_survey_id_seq TO editor;


--
-- TOC entry 4649 (class 0 OID 0)
-- Dependencies: 286
-- Name: zipcode; Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON TABLE zipcode FROM PUBLIC;
REVOKE ALL ON TABLE zipcode FROM postgres;
GRANT ALL ON TABLE zipcode TO postgres;
GRANT ALL ON TABLE zipcode TO rds_superuser;
GRANT SELECT ON TABLE zipcode TO reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE zipcode TO editor;


-- Completed on 2016-03-25 10:22:45

--
-- PostgreSQL database dump complete
--

