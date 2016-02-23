#!/usr/bin/python
import psycopg2 as pg
import pandas as pd
import argparse
import datetime as dt
import logging

LOG_LEVEL=logging.INFO
# Set up logging
LOG_FORMAT ='%(levelname)-8s%(message)s'
logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)
START_DATE = '2010-07-24'
# START_DATE = '2015-07-24'


_conn = None

def connect():
    try:
        global _conn
        if _conn is None:
            _conn = pg.connect(
                # user="unite_user",
                # password="unite5432",
                user="a_reader",
                password="airbnb5432",
                host='airbnb.cphovrwl1wxm.us-east-1.rds.amazonaws.com',
                port=5432,
                database='airbnb')
            _conn.set_client_encoding('UTF8')
        return _conn
    except:
        logging.exception("Failed to connect to database")
conn = connect()

def export_city_data(city, project, format):
    logging.info(" ---- Exporting data for " + city)
    survey_ids = []
    survey_dates = []
    survey_comments = []
    sql_survey_ids = """
        select survey_id, survey_date, comment
        from survey s, search_area sa
        where s.search_area_id = sa.search_area_id
        and sa.name = %s
        and s.survey_date > %s
        and s.status = 1
        order by survey_id
    """
    cur = _conn.cursor()
    cur.execute(sql_survey_ids, (city, START_DATE,))
    rs = cur.fetchall()
    for row in rs:
        survey_ids.append(row[0])
        survey_dates.append(row[1])
        survey_comments.append(row[2])

    #survey_ids = [11, ]
    if project == "unite":
        sql_abbrev = """
        select abbreviation 
        from search_area
        where name = %s
        """
        cur = _conn.cursor()
        cur.execute(sql_abbrev, (city, ))
        rs = cur.fetchall()
        city_view = 'listing_' + rs[0][0]
        cur.close()
        sql = """
        select room_id, host_id, room_type, 
        borough, neighborhood, 
        reviews, overall_satisfaction, 
        accommodates, bedrooms, bathrooms,
        price, minstay,
        latitude, longitude,
        last_modified as collected
        from 
        """
        sql += city_view
        sql += """
        where survey_id = %(survey_id)s
        order by room_id
        """
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
    if format == "csv":
        for survey_id, survey_date, survey_comment in zip(survey_ids, survey_dates, survey_comments):
            csvfile =  "./" + project + "/ts_" + city_bar + "_" + str(survey_date) + ".csv"
            csvfile = csvfile.lower()
            df = pd.read_sql(sql, conn, 
                    #index_col="room_id",
                    params={"survey_id": survey_id}
                    )
            logging.info("CSV export: survey " + str(survey_id) + " to " + csvfile)
            df.to_csv(csvfile)
    else:
        today = dt.date.today().isoformat() 
        xlsxfile =  "./" + project + "/slee_" + project + "_" + city_bar + "_" + today + ".xlsx"
        writer = pd.ExcelWriter(xlsxfile, engine="xlsxwriter")
        logging.info ("Spreadsheet name: " + xlsxfile)
        # read surveys
        for survey_id, survey_date, survey_comment in zip(survey_ids, survey_dates, survey_comments):
            logging.info("Survey " + str(survey_id) + " for " + city)
            df = pd.read_sql(sql, conn, 
                        #index_col="room_id",
                        params={"survey_id": survey_id}
                        )
            if len(df) > 0:
                logging.info("Survey " + str(survey_id) + ": to Excel worksheet")
                if survey_comment:
                    # This comment feature is not currently doing anything
                    options = {
                        'width': 256,
                        'height': 100,
                        'x_offset': 10,
                        'y_offset': 10,
                        'font': {'color': 'red',
                                'size': 14},
                        'align': {'vertical': 'middle',
                                'horizontal': 'center'
                                },
                    }
                df.to_excel(writer, sheet_name=str(survey_date))
            else:
                logging.info("Survey " + str(survey_id) + " not in production project: ignoring")

        # neighborhood summaries
        if project=="unite":
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
                    dfnb = df.pivot(index='neighborhood', columns='survey_date', values='listings')
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
                dfnb = df.pivot(index='neighborhood', columns='survey_date', values='visits')
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
    parser = \
        argparse.ArgumentParser(
            description="Create a spreadsheet of surveys from a city")
    parser.add_argument('-c', '--city',
                       metavar='city', action='store', 
                       help="""set the city""")
    parser.add_argument('-p', '--project',
                       metavar='project', action='store', default="public",
                       help="""the project determines the table or view""")
    parser.add_argument('-f', '--format',
                       metavar='format', action='store', default="xlsx",
                       help="""output format (xlsx or csv)""")
    args = parser.parse_args()

    if args.city:
        export_city_data(args.city, args.project.lower(), args.format)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
