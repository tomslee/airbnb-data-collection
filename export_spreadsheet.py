#!/usr/bin/python
import psycopg2 as pg
import pandas as pd
import argparse
import datetime as dt
import logging
import sys
import configparser
import os

LOG_LEVEL = logging.INFO
# Set up logging
LOG_FORMAT = '%(levelname)-8s%(message)s'
logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)
START_DATE = '2013-05-02'
# START_DATE = '2016-08-31'


def init():
    """ Read the configuration file <username>.config to set up the run.
    Copied from airbnb.py
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
        DB_HOST = config["DATABASE"]["db_host"]
        DB_PORT = config["DATABASE"]["db_port"]
        DB_NAME = config["DATABASE"]["db_name"]
        DB_USER = config["DATABASE"]["db_user"]
        DB_PASSWORD = config["DATABASE"]["db_password"]
        # network
        global USER_AGENT_LIST, HTTP_PROXY_LIST, MAX_CONNECTION_ATTEMPTS
        global REQUEST_SLEEP, HTTP_TIMEOUT
        try:
            HTTP_PROXY_LIST = config["NETWORK"]["proxy_list"].split(",")
            HTTP_PROXY_LIST = [x.strip() for x in HTTP_PROXY_LIST]
        except Exception:
            logging.info("No http_proxy_list in " + username +
                         ".config: not using proxies")
            HTTP_PROXY_LIST = []
        try:
            USER_AGENT_LIST = config["NETWORK"]["user_agent_list"].split(",,")
            USER_AGENT_LIST = [x.strip() for x in USER_AGENT_LIST]
            USER_AGENT_LIST = [x.strip('"') for x in USER_AGENT_LIST]
        except Exception:
            logging.info("No http_proxy_list in " + username +
                         ".config: not using proxies")
            HTTP_PROXY_LIST = []
        MAX_CONNECTION_ATTEMPTS = \
            int(config["NETWORK"]["max_connection_attempts"])
        REQUEST_SLEEP = float(config["NETWORK"]["request_sleep"])
        HTTP_TIMEOUT = float(config["NETWORK"]["http_timeout"])
        # survey
        global FILL_MAX_ROOM_COUNT, ROOM_ID_UPPER_BOUND, SEARCH_MAX_PAGES
        global SEARCH_MAX_GUESTS, SEARCH_MAX_RECTANGLE_ZOOM, RE_INIT_SLEEP_TIME
        FILL_MAX_ROOM_COUNT = int(config["SURVEY"]["fill_max_room_count"])
        ROOM_ID_UPPER_BOUND = int(config["SURVEY"]["room_id_upper_bound"])
        SEARCH_MAX_PAGES = int(config["SURVEY"]["search_max_pages"])
        SEARCH_MAX_GUESTS = int(config["SURVEY"]["search_max_guests"])
        SEARCH_MAX_RECTANGLE_ZOOM = int(
            config["SURVEY"]["search_max_rectangle_zoom"])
        RE_INIT_SLEEP_TIME = float(config["SURVEY"]["re_init_sleep_time"])
    except Exception:
        logging.exception("Failed to read config file properly")
        raise


def connect():
    """ Return a connection to the database"""
    try:
        if not hasattr(connect, "conn"):
            connect.conn = pg.connect(
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME)
            connect.conn.set_client_encoding('UTF8')
        elif connect.conn is None or connect.conn.closed != 0:
            connect.conn = pg.connect(
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME)
            connect.conn.set_client_encoding('UTF8')
        return connect.conn
    except pg.OperationalError as pgoe:
        logging.error(pgoe.message)
        raise
    except Exception:
        logging.error("Failed to connect to database.")
        raise


def survey_df(city):
    sql_survey_ids = """
        select survey_id, survey_date, comment
        from survey s, search_area sa
        where s.search_area_id = sa.search_area_id
        and sa.name = %(name)s
        and s.survey_date > %(date)s
        and s.status = 1
        order by survey_id
    """
    conn = connect()
    df = pd.read_sql(sql_survey_ids, conn,
                     params={"name": city, "date": START_DATE})
    conn.close()
    return(df)


def city_view_name(city):
    sql_abbrev = """
    select abbreviation from search_area
    where name = %s
    """
    conn = connect()
    cur = conn.cursor()
    cur.execute(sql_abbrev, (city, ))
    city_view_name = 'listing_' + cur.fetchall()[0][0]
    cur.close()
    return city_view_name


def total_listings(city_view):
    sql = """select s.survey_id "Survey",
    survey_date "Date", count(*) "Listings"
    from {city_view} r join survey s
    on r.survey_id = s.survey_id
    group by 1, 2
    order by 1
    """.format(city_view=city_view)
    conn = connect()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def by_room_type(city_view):
    sql = """select s.survey_id "Survey",
    survey_date "Date", room_type "Room Type",
    count(*) "Listings", sum(reviews) "Reviews",
    sum(reviews * price) "Relative Income"
    from {city_view} r join survey s
    on r.survey_id = s.survey_id
    where room_type is not null
    group by 1, 2, 3
    order by 1
    """.format(city_view=city_view)
    conn = connect()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df.pivot(index="Date", columns="Room Type")


def by_host_type(city_view):
    sql = """
    select survey_id "Survey",
        survey_date "Date",
        case when listings_for_host = 1
        then 'Single' else 'Multi'
        end "Host Type",
        sum(hosts) "Hosts", sum(listings) "Listings", sum(reviews) "Reviews"
    from (
        select survey_id, survey_date,
        listings_for_host, count(*) hosts,
        sum(listings_for_host) listings, sum(reviews) reviews
        from  (
            select s.survey_id survey_id, survey_date,
            host_id, count(*) listings_for_host,
            sum(reviews) reviews
            from {city_view} r join survey s
            on r.survey_id = s.survey_id
            group by s.survey_id, survey_date, host_id
            ) T1
        group by 1, 2, 3
    ) T2
    group by 1, 2, 3
    """.format(city_view=city_view)
    conn = connect()
    df = pd.read_sql(sql, conn)
    conn.close()
    df = df.pivot(index="Date", columns="Host Type")
    # df.set_index(["Date"], drop=False, inplace=True)
    return df


def by_neighborhood(city_view):
    sql = """select
        s.survey_id, survey_date "Date", neighborhood "Neighborhood",
        count(*) "Listings", sum(reviews) "Reviews"
    from {city_view} r join survey s
    on r.survey_id = s.survey_id
    group by 1, 2, 3
    """.format(city_view=city_view)
    conn = connect()
    df = pd.read_sql(sql, conn)
    conn.close()
    df = df.pivot(index="Date", columns="Neighborhood")
    # df.set_index(["Date"], drop=False, inplace=True)
    return df


def export_city_summary(city, project):
    logging.info(" ---- Exporting summary spreadsheet" +
                 " for " + city +
                 " using project " + project)
    city_bar = city.replace(" ", "_").lower()
    today = dt.date.today().isoformat()
    xlsxfile = ("./{project}/slee_{project}_{city_bar}_summary_{today}.xlsx"
                ).format(project=project, city_bar=city_bar, today=today)
    writer = pd.ExcelWriter(xlsxfile, engine="xlsxwriter")
    df = survey_df(city)
    city_view = city_view_name(city)
    logging.info("Total listings...")
    df = total_listings(city_view)
    df.to_excel(writer, sheet_name="Total Listings", index=False)
    logging.info("Listings by room type...")
    df = by_room_type(city_view)
    df["Listings"].to_excel(writer,
                            sheet_name="Listings by room type", index=True)
    df["Reviews"].to_excel(writer,
                           sheet_name="Reviews by room type", index=True)
    logging.info("Listings by host type...")
    df = by_host_type(city_view)
    df["Hosts"].to_excel(writer,
                         sheet_name="Hosts by host type", index=True)
    df["Listings"].to_excel(writer,
                            sheet_name="Listings by host type", index=True)
    df["Reviews"].to_excel(writer,
                           sheet_name="Reviews by host type", index=True)
    logging.info("Listings by neighborhood...")
    df = by_neighborhood(city_view)
    df["Listings"].to_excel(writer,
                            sheet_name="Listings by Neighborhood", index=True)
    df["Reviews"].to_excel(writer,
                           sheet_name="Reviews by Neighborhood", index=True)
    logging.info("Saving " + xlsxfile)
    writer.save()


def export_city_data(city, project, format):
    logging.info(" ---- Exporting " + format +
                 " for " + city +
                 " using project " + project)

    df = survey_df(city)
    survey_ids = df["survey_id"].tolist()
    survey_dates = df["survey_date"].tolist()
    logging.info(" ---- Surveys: " + ', '.join(str(id) for id in survey_ids))

    # survey_ids = [11, ]
    if project == "gis":
        city_view = city_view_name(city)
        sql = """
        select room_id, host_id, room_type,
            borough, neighborhood,
            reviews, overall_satisfaction,
            accommodates, bedrooms, bathrooms,
            price, minstay,
            latitude, longitude,
            last_modified as collected
        from {city_view}
        where survey_id = %(survey_id)s
        order by room_id
        """.format(city_view=city_view)
    elif project == "hvs":
        sql = """
        select room_id, host_id, room_type,
            borough, neighborhood,
            reviews, overall_satisfaction,
            accommodates, bedrooms, bathrooms,
            price, minstay,
            latitude, longitude,
            last_modified as collected
        from hvs.listing
        where survey_id = %(survey_id)s
        order by room_id
        """
    else:
        sql = """
        select room_id, host_id, room_type,
            city, neighborhood,
            reviews, overall_satisfaction,
            accommodates, bedrooms, bathrooms,
            price, minstay,
            latitude, longitude,
            last_modified as collected
        from survey_room(%(survey_id)s)
        order by room_id
        """

    city_bar = city.replace(" ", "_").lower()
    conn = connect()
    if format == "csv":
        for survey_id, survey_date in \
                zip(survey_ids, survey_dates):
            csvfile = ("./{project}/ts_{city_bar}_{survey_date}.csv").format(
                project=project, city_bar=city_bar,
                survey_date=str(survey_date))
            csvfile = csvfile.lower()
            df = pd.read_sql(sql, conn,
                             # index_col="room_id",
                             params={"survey_id": survey_id.item()}
                             )
            logging.info("CSV export: survey " +
                         str(survey_id) + " to " + csvfile)
            df.to_csv(csvfile)
            # default encoding is 'utf-8' on Python 3
    else:
        today = dt.date.today().isoformat()
        xlsxfile = ("./{project}/slee_{project}_{city_bar}_{today}.xlsx"
                    ).format(project=project, city_bar=city_bar, today=today)
        writer = pd.ExcelWriter(xlsxfile, engine="xlsxwriter")
        logging.info("Spreadsheet name: " + xlsxfile)
        # read surveys
        for survey_id, survey_date in \
                zip(survey_ids, survey_dates):
            logging.info("Survey " + str(survey_id) + " for " + city)
            df = pd.read_sql(sql, conn,
                             # index_col="room_id",
                             params={"survey_id": survey_id.item()}
                             )
            if len(df) > 0:
                logging.info("Survey " + str(survey_id) +
                             ": to Excel worksheet")
                df.to_excel(writer, sheet_name=str(survey_date))
            else:
                logging.info("Survey " + str(survey_id) +
                             " not in production project: ignoring")

        # neighborhood summaries
        if project == "gis":
            sql = "select to_char(survey_date, 'YYYY-MM-DD') as survey_date,"
            sql += " neighborhood, count(*) as listings from"
            sql += " " + city_view + " li,"
            sql += " survey s"
            sql += " where li.survey_id = s.survey_id"
            sql += " and s.survey_date > %(start_date)s"
            sql += " group by survey_date, neighborhood order by 3 desc"
            try:
                df = pd.read_sql(sql, conn, params={"start_date": START_DATE})
                if len(df.index) > 0:
                    logging.info("Exporting listings for " + city)
                    dfnb = df.pivot(index='neighborhood', columns='survey_date',
                                    values='listings')
                    dfnb.fillna(0)
                    dfnb.to_excel(writer, sheet_name="Listings by neighborhood")
            except pg.InternalError:
                # Miami has no neighborhoods
                pass
            except pd.io.sql.DatabaseError:
                # Miami has no neighborhoods
                pass

        sql = "select to_char(survey_date, 'YYYY-MM-DD') as survey_date,"
        sql += " neighborhood, sum(reviews) as visits from"
        sql += " " + city_view + " li,"
        sql += " survey s"
        sql += " where li.survey_id = s.survey_id"
        sql += " and s.survey_date > %(start_date)s"
        sql += " group by survey_date, neighborhood order by 3  desc"
        try:
            df = pd.read_sql(sql, conn, params={"start_date": START_DATE})
            if len(df.index) > 0:
                logging.info("Exporting visits for " + city)
                dfnb = df.pivot(index='neighborhood', columns='survey_date',
                                values='visits')
                dfnb.fillna(0)
                dfnb.to_excel(writer, sheet_name="Visits by neighborhood")
        except pg.InternalError:
            # Miami has no neighborhoods
            pass
        except pd.io.sql.DatabaseError:
            pass

        logging.info("Saving " + xlsxfile)
        writer.save()


def main():
    init()
    parser = \
        argparse.ArgumentParser(
            description="Create a spreadsheet of surveys from a city")
    parser.add_argument('-c', '--city',
                        metavar='city', action='store',
                        help="""set the city""")
    parser.add_argument('-p', '--project',
                        metavar='project', action='store', default="public",
                        help="""the project determines the table or view: public
                        for room, gis for listing_city, default public""")
    parser.add_argument('-f', '--format',
                        metavar='format', action='store', default="xlsx",
                        help="""output format (xlsx or csv), default xlsx""")
    parser.add_argument('-s', '--summary',
                        action='store_true', default=False,
                        help="create a summary spreadsheet instead of raw data")
    args = parser.parse_args()

    if args.city:
        if args.summary:
            export_city_summary(args.city, args.project.lower())
        else:
            export_city_data(args.city, args.project.lower(), args.format)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
