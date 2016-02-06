--
-- PostgreSQL database dump
--

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
-- Name: city; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE city (
    city_id integer NOT NULL,
    name character varying(255),
    search_area_id integer
);


ALTER TABLE city OWNER TO postgres;

--
-- Name: city_city_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE city_city_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE city_city_id_seq OWNER TO postgres;

--
-- Name: city_city_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE city_city_id_seq OWNED BY city.city_id;


--
-- Name: city_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY city ALTER COLUMN city_id SET DEFAULT nextval('city_city_id_seq'::regclass);


--
-- Name: city_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY city
    ADD CONSTRAINT city_pkey PRIMARY KEY (city_id);


--
-- Name: city; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE city FROM PUBLIC;
REVOKE ALL ON TABLE city FROM postgres;
GRANT ALL ON TABLE city TO postgres;
GRANT SELECT ON TABLE city TO reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE city TO editor;


--
-- Name: city_city_id_seq; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON SEQUENCE city_city_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE city_city_id_seq FROM postgres;
GRANT ALL ON SEQUENCE city_city_id_seq TO postgres;
GRANT SELECT,USAGE ON SEQUENCE city_city_id_seq TO editor;


--
-- PostgreSQL database dump complete
--

