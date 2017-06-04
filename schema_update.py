#!/usr/bin/python3
# ============================================================================
# Manage the schema for the Airbnb data scraper
# Tom Slee, 2013--2015.
#
import logging
import sys
import os
import psycopg2
import psycopg2.errorcodes
import configparser

# ============================================================================
# CONSTANTS
# ============================================================================
# Database: filled in from <username>.config
DB_HOST = None
DB_PORT = None
DB_NAME = None
DB_USER = None
DB_PASSWORD = None

LOG_LEVEL = logging.DEBUG
# LOG_LEVEL = logging.INFO
# Set up logging
logger = logging.getLogger()
# Suppress informational logging from requests module
logger.setLevel(LOG_LEVEL)

ch_formatter = logging.Formatter('%(levelname)-8s%(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(LOG_LEVEL)
console_handler.setFormatter(ch_formatter)
logger.addHandler(console_handler)

fl_formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s%(message)s')
filelog_handler = logging.FileHandler("run.log", encoding="utf-8")
filelog_handler.setLevel(LOG_LEVEL)
filelog_handler.setFormatter(fl_formatter)
logger.addHandler(filelog_handler)


def init():
    """ Read the configuration file <username>.config to set up the run
    """
    try:
        config = configparser.ConfigParser()
        # look for username.config on both Windows (USERNAME) and Linux (USER)
        if os.name == "nt":
            username = os.environ['USERNAME']
        else:
            username = os.environ['USER']
        config_file = username + ".config"
        if not os.path.isfile(config_file):
            logging.error("Configuration file " + config_file + " not found.")
            sys.exit()
        config.read(config_file)
        # database
        global DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
        DB_HOST = config["DATABASE"]["db_host"] if ("db_host" in config["DATABASE"]) else None
        DB_PORT = config["DATABASE"]["db_port"]
        DB_NAME = config["DATABASE"]["db_name"]
        DB_USER = config["DATABASE"]["db_user"]
        DB_PASSWORD = config["DATABASE"]["db_password"]
    except Exception:
        logger.exception("Failed to read config file properly")
        raise


def confirm(prompt=None, resp=False):
    """Cut and paste from
    http://code.activestate.com/recipes/541096-prompt-the-user-for-confirmation/.
    Prompts for yes or no response from the user. Returns True for yes and
    False for no.

    'resp' should be set to the default value assumed by the caller when
    user simply types ENTER.

    >>> confirm(prompt='Create Directory?', resp=True)
    Create Directory? [y]|n:
    True
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y:
    False
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: y
    True

    """
    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')
    while True:
        ans = raw_input(prompt)
        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            print('please enter y or n.')
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False


def connect():
    """ Return a connection to the database"""
    try:
        if (not hasattr(connect, "conn") or connect.conn is None or connect.conn.closed != 0):
            cattr = dict(
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            if DB_HOST is not None:
                cattr.update(dict(
                            host=DB_HOST,
                            port=DB_PORT,
                            ))
            connect.conn = psycopg2.connect(**cattr)
            connect.conn.set_client_encoding('UTF8')
        return connect.conn
    except psycopg2.OperationalError as pgoe:
        logger.error(pgoe.message)
        raise
    except Exception:
        logger.error("Failed to connect to database.")
        raise


def get_schema_version():
    try:
        sql = """select version from schema_version"""
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        version = cur.fetchone()[0]
        cur.close()
        conn.commit()
        return version
    except:
        return -1


def fix_version_table():
    try:
        sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='schema_version' and column_name='version'
        """
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        version_col = cur.fetchone()[0]
        if version_col:
            logger.info("Check: schema_version table already has version column")
        cur.close()
        conn.commit()
    except:
        logger.info("Creating schema_version table...")
        sql = """
        create table schema_version (
            version numeric(5,2) primary key
        )
        """
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        conn.commit()

def add_survey_log_bb_table():
    try:
        sql = """
        create table survey_progress_log_bb (
	    survey_id integer primary key,
	    room_type varchar(255),
	    guests integer,
	    price_min float,
	    price_max float,
	    quadtree_node varchar(1024),
            last_modified timestamp without time zone default now()
        )
        """
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        conn.commit()

def fix_room_table():
    try:
        sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='room' and column_name='room_id'
        """
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        test_room_id = cur.fetchone()[0]
        if test_room_id:
            logger.info("Check: room table already has room_id column")
        cur.close()
        conn.commit()
    except psycopg2.Error as e:
        template = "Exception of type {0}. Arguments:\n{1!r}"
        message = template.format(type(e).__name__, e.args)
        print(message)
        # create the first pass at the room table
        conn.commit()
        logger.info("Check: room table has no room_id column")
        logger.info("Creating table 'room'...")
        sql = """
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
          survey_id integer NOT NULL DEFAULT 999999,
          location geometry,
          CONSTRAINT room_pkey PRIMARY KEY (room_id, survey_id)
          )
        """
        if confirm(prompt='Create table "room"?', resp=False):
            conn = connect()
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
            conn.commit()
        else:
            print("Table 'room' not created")

    # November 2016: added several columns
    try:
        sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='room' and column_name='coworker_hosted'
        """
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        test_coworker_hosted_id = cur.fetchone()[0]
        if test_coworker_hosted_id:
            logger.info("Check: room table already has coworker_hosted column")
        cur.close()
        conn.commit()
    except:
        logger.info("Check: room table has no coworker_hosted column")
        logger.info("Altering table room")
        conn = connect()
        sql = """
        alter table room
        add column coworker_hosted integer,
        add column extra_host_languages character varying(255),
        add column "name" character varying(255),
        add column property_type character varying(255),
        add column currency character varying(20),
        add column rate_type character varying(20);
        """
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        sql = """
        alter table survey_progress_log
        add column zoomstack character varying(255)
        """
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        conn.commit()


# -----------------------------------------------------------------------------
# SQL listings for schema maintenance
# -----------------------------------------------------------------------------


def main():
    init()
    fix_version_table()
    fix_room_table()


if __name__ == "__main__":
    main()
