#!/usr/bin/python3
# ============================================================================
# Airbnb web site scraper, for analysis of Airbnb listings
# Tom Slee, 2013--2015.
#
# function naming conventions:
#   ws_get = get from web site
#   db_get = get from database
#   db_add = add to the database
#
# function name conventions:
#   add = add to database
#   display = open a browser and show
#   list = get from database and print
#   print = get from web site and print
# ============================================================================
import logging
import argparse
import sys
import time
import webbrowser
from lxml import html
import psycopg2
import psycopg2.errorcodes
from airbnb_config import ABConfig
from airbnb_survey import ABSurveyByBoundingBox
from airbnb_survey import ABSurveyByNeighborhood, ABSurveyByZipcode
from airbnb_listing import ABListing
import airbnb_ws

# ============================================================================
# CONSTANTS
# ============================================================================

# Script version
# 3.3 April 2018: Changed to use /api/ for -sb if key provided in config file
# 3.2 April 2018: fix for modified Airbnb site. Avoided loops over room types
#                 in -sb
# 3.1 provides more efficient "-sb" searches, avoiding loops over guests and
# prices. See example.config for details, and set a large max_zoom (eg 12).
# 3.0 modified -sb searches to reflect new Airbnb web site design (Jan 2018)
# 2.9 adds resume for bounding box searches. Requires new schema
# 2.8 makes different searches subclasses of ABSurvey
# 2.7 factors the Survey and Listing objects into their own modules
# 2.6 adds a bounding box search
# 2.5 is a bit of a rewrite: classes for ABListing and ABSurvey, and requests lib
# 2.3 released Jan 12, 2015, to handle a web site update
SCRIPT_VERSION_NUMBER = 3.3
# logging = logging.getLogger()

def list_search_area_info(config, search_area):
    """
    Print a list of the search areas in the database to stdout.
    """
    try:
        conn = config.connect()
        cur = conn.cursor()
        cur.execute("""
                select search_area_id
                from search_area where name=%s
                """, (search_area,))
        result_set = cur.fetchall()
        cur.close()
        count = len(result_set)
        if count == 1:
            print("\nThere is one search area called",
                  str(search_area),
                  "in the database.")
        elif count > 1:
            print("\nThere are", str(count),
                  "cities called", str(search_area),
                  "in the database.")
        elif count < 1:
            print("\nThere are no cities called",
                  str(search_area),
                  "in the database.")
            sys.exit()
        sql_neighborhood = """select count(*) from neighborhood
        where search_area_id = %s"""
        sql_search_area = """select count(*) from search_area
        where search_area_id = %s"""
        for result in result_set:
            search_area_id = result[0]
            cur = conn.cursor()
            cur.execute(sql_neighborhood, (search_area_id,))
            count = cur.fetchone()[0]
            cur.close()
            print("\t" + str(count) + " neighborhoods.")
            cur = conn.cursor()
            cur.execute(sql_search_area, (search_area_id,))
            count = cur.fetchone()[0]
            cur.close()
            print("\t" + str(count) + " Airbnb cities.")
    except psycopg2.Error as pge:
        logging.error(pge.pgerror)
        logging.error("Error code %s", pge.pgcode)
        logging.error("Diagnostics %s", pge.diag.message_primary)
        cur.close()
        conn.rollback()
        raise
    except Exception:
        logging.error("Failed to list search area info")
        raise


def list_surveys(config):
    """
    Print a list of the surveys in the database to stdout.
    """
    try:
        conn = config.connect()
        cur = conn.cursor()
        cur.execute("""
            select survey_id, to_char(survey_date, 'YYYY-Mon-DD'),
                    survey_description, search_area_id, status
            from survey
            where survey_date is not null
            and status is not null
            and survey_description is not null
            order by survey_id asc""")
        result_set = cur.fetchall()
        if result_set:
            template = "| {0:3} | {1:>12} | {2:>50} | {3:3} | {4:3} |"
            print (template.format("ID", "Date", "Description", "SA", "status"))
            for survey in result_set:
                (survey_id, survey_date, desc, sa_id, status) = survey
                print(template.format(survey_id, survey_date, desc, sa_id, status))
    except Exception:
        logging.error("Cannot list surveys.")
        raise


def db_ping(config):
    """
    Test database connectivity, and print success or failure.
    """
    try:
        conn = config.connect()
        if conn is not None:
            print("Connection test succeeded: {db_name}@{db_host}"
                  .format(db_name=config.DB_NAME, db_host=config.DB_HOST))
        else:
            print("Connection test failed")
    except Exception:
        logging.exception("Connection test failed")


def db_add_survey(config, search_area):
    """
    Add a survey entry to the database, so the survey can be run.
    """
    try:
        conn = config.connect()
        cur = conn.cursor()
        # Add an entry into the survey table, and get the survey_id
        sql = """
        insert into survey (survey_description, search_area_id)
        select (name || ' (' || current_date || ')') as survey_description,
        search_area_id
        from search_area
        where name = %s
        returning survey_id"""
        cur.execute(sql, (search_area,))
        survey_id = cur.fetchone()[0]

        # Get and print the survey entry
        cur.execute("""select survey_id, survey_date,
        survey_description, search_area_id
        from survey where survey_id = %s""", (survey_id,))
        (survey_id,
         survey_date,
         survey_description,
         search_area_id) = cur.fetchone()
        conn.commit()
        cur.close()
        print("\nSurvey added:\n"
              + "\n\tsurvey_id=" + str(survey_id)
              + "\n\tsurvey_date=" + str(survey_date)
              + "\n\tsurvey_description=" + survey_description
              + "\n\tsearch_area_id=" + str(search_area_id))
    except Exception:
        logging.error("Failed to add survey for %s", search_area)
        raise


def db_get_room_to_fill(config, survey_id):
    """
    For "fill" runs (loops over room pages), choose a random room that has
    not yet been visited in this "fill".
    """
    for attempt in range(config.MAX_CONNECTION_ATTEMPTS):
        try:
            conn = config.connect()
            cur = conn.cursor()
            if survey_id == 0:  # no survey specified
                sql = """
                    select room_id, survey_id
                    from room
                    where deleted is null
                    order by random()
                    limit 1
                    """
                cur.execute(sql)
            else:
                sql = """
                    select room_id, survey_id
                    from room
                    where deleted is null
                    and survey_id = %s
                    order by random()
                    limit 1
                    """
                cur.execute(sql, (survey_id,))
            (room_id, survey_id) = cur.fetchone()
            listing = ABListing(config, room_id, survey_id)
            cur.close()
            conn.commit()
            return listing
        except TypeError:
            logging.info("Finishing: no unfilled rooms in database --")
            conn.rollback()
            del config.connection
            return None
        except Exception:
            logging.exception("Error retrieving room to fill from db")
            conn.rollback()
            del config.connection
    return None


def ws_get_city_info(config, city, flag):
    """
    Get city information from Airbnb web site, when adding a city
    (search area) to the database.
    """
    try:
        logging.info("Adding city to database as new search area")
        # Add the city to the database anyway
        conn = config.connect()
        cur = conn.cursor()
        # check if it exists
        sql = """
        select name
        from search_area
        where name = %s"""
        cur.execute(sql, (city,))
        if cur.fetchone() is not None:
            logging.info("City already exists: %s", city)
            return True
        # Insert the city into the table
        sql = """insert
        into search_area (name)
        values (%s)"""
        cur.execute(sql, (city,))
        sql = """select
        currval('search_area_search_area_id_seq')
        """
        cur.execute(sql, ())
        search_area_id = cur.fetchone()[0]
        # city_id = cur.lastrowid
        logging.info("City %s added: search_area_id = %d", city, search_area_id)

        url = config.URL_SEARCH_ROOT + city
        logging.debug(url)
        response = airbnb_ws.ws_request_with_repeats(config, url)
        if response is None:
            return False
        tree = html.fromstring(response.text)
        try:
            citylist = tree.xpath(
                "//input[@name='location']/@value")
            neighborhoods = tree.xpath(
                "//input[contains(@id, 'filter-option-neighborhoods')]/@value")
            if flag == config.FLAGS_PRINT:
                print("\n", citylist[0])
                print("Neighborhoods:")
                for neighborhood in neighborhoods:
                    print("\t", neighborhood)
            elif flag == config.FLAGS_ADD:
                if citylist:
                    sql_city = """insert
                            into city (name, search_area_id)
                            values (%s,%s)"""
                    cur.execute(sql_city, (city, search_area_id,))
                    logging.debug("%s neighborhoods", str(len(neighborhoods)))
                if neighborhoods:
                    sql_neighborhood = """
                        insert into neighborhood(name, search_area_id)
                        values(%s, %s)
                        """
                    for neighborhood in neighborhoods:
                        cur.execute(sql_neighborhood, (neighborhood,
                                                       search_area_id,))
                        logging.info("Added neighborhood %s", neighborhood)
                else:
                    logging.info("No neighborhoods found for %s", city)
                conn.commit()
        except UnicodeEncodeError:
            # if sys.version_info >= (3,):
            #    logging.info(s.encode('utf8').decode(sys.stdout.encoding))
            # else:
            #    logging.info(s.encode('utf8'))
            # unhandled at the moment
            pass
        except Exception:
            logging.error("Error collecting city and neighborhood information")
            raise
        return True
    except Exception:
        logging.error("Error getting city info from website")
        raise


def display_room(config, room_id):
    """
    Open a web browser and show the listing page for a room.
    """
    webbrowser.open(config.URL_ROOM_ROOT + str(room_id))


def display_host(config, host_id):
    """
    Open a web browser and show the user page for a host.
    """
    webbrowser.open(config.URL_HOST_ROOT + str(host_id))


def fill_loop_by_room(config, survey_id):
    """
    Master routine for looping over rooms (after a search)
    to fill in the properties.
    """
    room_count = 0
    while room_count < config.FILL_MAX_ROOM_COUNT:
        try:
            if not config.HTTP_PROXY_LIST:
                logging.info(
                    "No proxies left: re-initialize after %s seconds",
                    config.RE_INIT_SLEEP_TIME)
                time.sleep(config.RE_INIT_SLEEP_TIME)  # be nice
                config = ABConfig()
            room_count += 1
            listing = db_get_room_to_fill(config, survey_id)
            if listing is None:
                return None
            else:
                if listing.ws_get_room_info(config.FLAGS_ADD):
                    pass
                else:  # Airbnb now seems to return nothing if a room has gone
                    listing.save_as_deleted()
        except AttributeError:
            logging.error("Attribute error: marking room as deleted.")
            listing.save_as_deleted()
        except Exception as e:
            logging.error("Error in fill_loop_by_room: %s", str(type(e)))
            raise


def parse_args():
    """
    Read and parse command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Manage a database of Airbnb listings.',
        usage='%(prog)s [options]')
    parser.add_argument("-v", "--verbose",
                        action="store_true", default=False,
                        help="""write verbose (debug) output to the log file""")
    parser.add_argument("-c", "--config_file",
                        metavar="config_file", action="store", default=None,
                        help="""explicitly set configuration file, instead of
                        using the default <username>.config""")
    # Only one argument!
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-asa', '--addsearcharea',
                       metavar='search_area', action='store', default=False,
                       help="""get and save the name and neighborhoods
                       for search area (city)""")
    group.add_argument('-asv', '--addsurvey',
                       metavar='search_area', type=str,
                       help="""add a survey entry to the database,
                       for search_area""")
    group.add_argument('-dbp', '--dbping',
                       action='store_true', default=False,
                       help='Test the database connection')
    group.add_argument('-dh', '--displayhost',
                       metavar='host_id', type=int,
                       help='display web page for host_id in browser')
    group.add_argument('-dr', '--displayroom',
                       metavar='room_id', type=int,
                       help='display web page for room_id in browser')
    group.add_argument('-f', '--fill', nargs='?',
                       metavar='survey_id', type=int, const=0,
                       help='fill details for rooms collected with -s')
    group.add_argument('-lsa', '--listsearcharea',
                       metavar='search_area', type=str,
                       help="""list information about this search area
                       from the database""")
    group.add_argument('-lr', '--listroom',
                       metavar='room_id', type=int,
                       help='list information about room_id from the database')
    group.add_argument('-ls', '--listsurveys',
                       action='store_true', default=False,
                       help='list the surveys in the database')
    group.add_argument('-psa', '--printsearcharea',
                       metavar='search_area', action='store', default=False,
                       help="""print the name and neighborhoods for
                       search area (city) from the Airbnb web site""")
    group.add_argument('-pr', '--printroom',
                       metavar='room_id', type=int,
                       help="""print room_id information
                       from the Airbnb web site""")
    group.add_argument('-ps', '--printsearch',
                       metavar='survey_id', type=int,
                       help="""print first page of search information
                       for survey from the Airbnb web site""")
    group.add_argument('-psn', '--printsearch_by_neighborhood',
                       metavar='survey_id', type=int,
                       help="""print first page of search information
                       for survey from the Airbnb web site,
                       by neighborhood""")
    group.add_argument('-psz', '--printsearch_by_zipcode',
                       metavar='survey_id', type=int,
                       help="""print first page of search information
                       for survey from the Airbnb web site,
                       by zipcode""")
    group.add_argument('-psb', '--printsearch_by_bounding_box',
                       metavar='survey_id', type=int,
                       help="""print first page of search information
                       for survey from the Airbnb web site,
                       by bounding_box""")
    group.add_argument('-s', '--search',
                       metavar='survey_id', type=int,
                       help='search for rooms using survey survey_id')
    group.add_argument('-sn', '--search_by_neighborhood',
                       metavar='survey_id', type=int,
                       help='search for rooms using survey survey_id')
    group.add_argument('-sb', '--search_by_bounding_box',
                       metavar='survey_id', type=int,
                       help="""search for rooms using survey survey_id,
                       by bounding box
                       """)
    group.add_argument('-sz', '--search_by_zipcode',
                       metavar='survey_id', type=int,
                       help="""search for rooms using survey_id,
                       by zipcode""")
    group.add_argument('-V', '--version',
                       action='version',
                       version='%(prog)s, version ' +
                       str(SCRIPT_VERSION_NUMBER))
    group.add_argument('-?', action='help')

    args = parser.parse_args()
    return (parser, args)


def main():
    """
    Main entry point for the program.
    """
    (parser, args) = parse_args()
    logging.basicConfig(format='%(levelname)-8s%(message)s')
    ab_config = ABConfig(args)
    try:
        if args.search:
            survey = ABSurveyByNeighborhood(ab_config, args.search)
            survey.search(ab_config.FLAGS_ADD)
        elif args.search_by_neighborhood:
            survey = ABSurveyByNeighborhood(ab_config, args.search_by_neighborhood)
            survey.search(ab_config.FLAGS_ADD)
        elif args.search_by_zipcode:
            survey = ABSurveyByZipcode(ab_config, args.search_by_zipcode)
            survey.search(ab_config.FLAGS_ADD)
        elif args.search_by_bounding_box:
            survey = ABSurveyByBoundingBox(ab_config, args.search_by_bounding_box)
            survey.search(ab_config.FLAGS_ADD)
        elif args.fill is not None:
            fill_loop_by_room(ab_config, args.fill)
        elif args.addsearcharea:
            ws_get_city_info(ab_config, args.addsearcharea, ab_config.FLAGS_ADD)
        elif args.addsurvey:
            db_add_survey(ab_config, args.addsurvey)
        elif args.dbping:
            db_ping(ab_config)
        elif args.displayhost:
            display_host(ab_config, args.displayhost)
        elif args.displayroom:
            display_room(ab_config, args.displayroom)
        elif args.listsearcharea:
            list_search_area_info(ab_config, args.listsearcharea)
        elif args.listroom:
            listing = ABListing(ab_config, args.listroom, None)
            listing.print_from_db()
        elif args.listsurveys:
            list_surveys(ab_config)
        elif args.printsearcharea:
            ws_get_city_info(ab_config, args.printsearcharea, ab_config.FLAGS_PRINT)
        elif args.printroom:
            listing = ABListing(ab_config, args.printroom, None)
            listing.ws_get_room_info(ab_config.FLAGS_PRINT)
        elif args.printsearch:
            survey = ABSurveyByNeighborhood(ab_config, args.printsearch)
            survey.search(ab_config.FLAGS_PRINT)
        elif args.printsearch_by_neighborhood:
            survey = ABSurveyByNeighborhood(ab_config, args.printsearch_by_neighborhood)
            survey.search(ab_config.FLAGS_PRINT)
        elif args.printsearch_by_bounding_box:
            survey = ABSurveyByBoundingBox(ab_config, args.printsearch_by_bounding_box)
            survey.search(ab_config.FLAGS_PRINT)
        elif args.printsearch_by_zipcode:
            survey = ABSurveyByZipcode(ab_config, args.printsearch_by_zipcode)
            survey.search(ab_config.FLAGS_PRINT)
        else:
            parser.print_help()
    except (SystemExit, KeyboardInterrupt):
        sys.exit()
    except Exception:
        logging.exception("Top level exception handler: quitting.")
        sys.exit(0)

if __name__ == "__main__":
    main()
