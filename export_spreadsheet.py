#!/usr/bin/python
import psycopg2 as pg
import pandas as pd
import argparse
import datetime as dt
import logging
from airbnb_config import ABConfig

LOG_LEVEL = logging.INFO
# Set up logging
LOG_FORMAT = '%(levelname)-8s%(message)s'
logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)
START_DATE = '2013-05-02'
# START_DATE = '2016-08-31'


def survey_df(ab_config, city):
    sql_survey_ids = """
        select survey_id, survey_date, comment
        from survey s, search_area sa
        where s.search_area_id = sa.search_area_id
        and sa.name = %(name)s
        and s.survey_date > %(date)s
        and s.status = 1
        order by survey_id
    """
    conn = ab_config.connect()
    df = pd.read_sql(sql_survey_ids, conn,
                     params={"name": city, "date": START_DATE})
    conn.close()
    return(df)


def city_view_name(ab_config, city):
    sql_abbrev = """
    select abbreviation from search_area
    where name = %s
    """
    conn = ab_config.connect()
    cur = conn.cursor()
    cur.execute(sql_abbrev, (city, ))
    city_view_name = 'listing_' + cur.fetchall()[0][0]
    cur.close()
    return city_view_name


def total_listings(ab_config, city_view):
    sql = """select s.survey_id "Survey",
    survey_date "Date", count(*) "Listings"
    from {city_view} r join survey s
    on r.survey_id = s.survey_id
    group by 1, 2
    order by 1
    """.format(city_view=city_view)
    conn = ab_config.connect()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def by_room_type(ab_config, city_view):
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
    conn = ab_config.connect()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df.pivot(index="Date", columns="Room Type")


def by_host_type(ab_config, city_view):
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
    conn = ab_config.connect()
    df = pd.read_sql(sql, conn)
    conn.close()
    df = df.pivot(index="Date", columns="Host Type")
    # df.set_index(["Date"], drop=False, inplace=True)
    return df


def by_neighborhood(ab_config, city_view):
    sql = """select
        s.survey_id, survey_date "Date", neighborhood "Neighborhood",
        count(*) "Listings", sum(reviews) "Reviews"
    from {city_view} r join survey s
    on r.survey_id = s.survey_id
    group by 1, 2, 3
    """.format(city_view=city_view)
    conn = ab_config.connect()
    df = pd.read_sql(sql, conn)
    conn.close()
    df = df.pivot(index="Date", columns="Neighborhood")
    # df.set_index(["Date"], drop=False, inplace=True)
    return df


def export_city_summary(ab_config, city, project):
    logging.info(" ---- Exporting summary spreadsheet" +
                 " for " + city +
                 " using project " + project)
    city_bar = city.replace(" ", "_").lower()
    today = dt.date.today().isoformat()
    xlsxfile = ("./{project}/slee_{project}_{city_bar}_summary_{today}.xlsx"
                ).format(project=project, city_bar=city_bar, today=today)
    writer = pd.ExcelWriter(xlsxfile, engine="xlsxwriter")
    df = survey_df(ab_config, city)
    city_view = city_view_name(ab_config, city)
    logging.info("Total listings...")
    df = total_listings(ab_config, city_view)
    df.to_excel(writer, sheet_name="Total Listings", index=False)
    logging.info("Listings by room type...")
    df = by_room_type(ab_config, city_view)
    df["Listings"].to_excel(writer,
                            sheet_name="Listings by room type", index=True)
    df["Reviews"].to_excel(writer,
                           sheet_name="Reviews by room type", index=True)
    logging.info("Listings by host type...")
    df = by_host_type(ab_config, city_view)
    df["Hosts"].to_excel(writer,
                         sheet_name="Hosts by host type", index=True)
    df["Listings"].to_excel(writer,
                            sheet_name="Listings by host type", index=True)
    df["Reviews"].to_excel(writer,
                           sheet_name="Reviews by host type", index=True)
    logging.info("Listings by neighborhood...")
    df = by_neighborhood(ab_config, city_view)
    df["Listings"].to_excel(writer,
                            sheet_name="Listings by Neighborhood", index=True)
    df["Reviews"].to_excel(writer,
                           sheet_name="Reviews by Neighborhood", index=True)
    logging.info("Saving " + xlsxfile)
    writer.save()


def export_city_data(ab_config, city, project, format):
    logging.info(" ---- Exporting " + format +
                 " for " + city +
                 " using project " + project)

    df = survey_df(ab_config, city)
    survey_ids = df["survey_id"].tolist()
    survey_dates = df["survey_date"].tolist()
    logging.info(" ---- Surveys: " + ', '.join(str(id) for id in survey_ids))

    # survey_ids = [11, ]
    if project == "gis":
        city_view = city_view_name(ab_config, city)
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
    conn = ab_config.connect()
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
    ab_config = ABConfig()
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
            export_city_summary(ab_config, args.city, args.project.lower())
        else:
            export_city_data(ab_config, args.city, args.project.lower(), args.format)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
