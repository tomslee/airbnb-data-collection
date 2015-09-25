#!/usr/bin/python
import psycopg2 as pg
import pandas as pd
import argparse
import datetime as dt

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

def get_spreadsheet(city, schema, format):
    survey_ids = []
    survey_dates = []
    sql_survey_ids = """
        select survey_id, survey_date
        from survey s, search_area sa
        where s.search_area_id = sa.search_area_id
        and sa.name = %s
        order by survey_id
    """
    cur = _conn.cursor()
    cur.execute(sql_survey_ids, (city, ))
    rs = cur.fetchall()
    for row in rs:
        survey_ids.append(row[0])
        survey_dates.append(row[1])

    #survey_ids = [11, ]
    if schema == "unite":
        sql = """
        select room_id, host_id, room_type, 
        borough, neighborhood, 
        reviews, overall_satisfaction, 
        accommodates, bedrooms, bathrooms,
        price, minstay,
        latitude, longitude,
        last_modified as collected
        from unite.listing
        where survey_id = %(survey_id)s
        order by room_id
        """
    elif schema == "hvs":
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

    if format == "csv":
        for survey_id, survey_date in zip(survey_ids, survey_dates):
            csvfile =  "./" + schema + "/Slee_" + schema + "_" + city + "_" + str(survey_id) + ".csv"
            df = pd.read_sql(sql, conn, 
                    #index_col="room_id",
                    params={"survey_id": survey_id}
                    )
            print ("To CSV on survey " + str(survey_id))
            df.to_csv(csvfile)
    else:
        today = dt.date.today().isoformat() 
        city_bar = city.replace (" ", "_")
        xlsxfile =  "./" + schema + "/Slee_" + schema + "_" + city_bar + "_" + today + ".xlsx"
        writer = pd.ExcelWriter(xlsxfile, engine="xlsxwriter")
        print ("Starting on " + xlsxfile)
        for survey_id, survey_date in zip(survey_ids, survey_dates):
            print ("Starting on survey " + str(survey_id) + " for city " + city)
            df = pd.read_sql(sql, conn, 
                        #index_col="room_id",
                        params={"survey_id": survey_id}
                        )
            print ("To Excel on survey " + str(survey_id))
            df.to_excel(writer, sheet_name=str(survey_date))
        print ("Saving " + xlsxfile)
        writer.save()
    
def main():
    parser = \
        argparse.ArgumentParser(
            description="Create a spreadsheet of surveys from a city")
    parser.add_argument('-c', '--city',
                       metavar='city', action='store', 
                       help="""set the city""")
    parser.add_argument('-s', '--schema',
                       metavar='schema', action='store', default="public",
                       help="""the schema holding the listings table""")
    parser.add_argument('-f', '--format',
                       metavar='format', action='store', default="xlsx",
                       help="""output format (xlsx or csv)""")
    args = parser.parse_args()

    if args.city:
        get_spreadsheet(args.city, args.schema, args.format)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
