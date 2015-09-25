#!/usr/bin/python
import psycopg2 as pg
import pandas as pd

_conn = None

def connect():
    try:
        global _conn
        if _conn is None:
            _conn = pg.connect(
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
city = "New York"
# Get the surveys associated with this city
survey_ids = [11, 49, 56, 67, 100, 105, 129,]
survey_dates = ["2014-05-10", "2014-08-31", "2014-10-17", "2014-12-02",
              "2015-02-27", "2015-03-14", "2015-08-10"]
#survey_ids = [11, ]
sql = """
        select room_id, host_id, room_type, 
        borough, neighborhood, 
        reviews, overall_satisfaction, 
        accommodates, bedrooms, bathrooms,
        price, minstay,
        latitude, longitude,
        last_modified as collected
        from publish.listing 
        where survey_id = %(survey_id)s
        order by room_id
        """
#csvfile =  "survey_" + str(survey_id) + ".csv"
xlsxfile =  city + ".xlsx"
writer = pd.ExcelWriter(xlsxfile, engine="xlsxwriter")
#writer = pd.ExcelWriter(xlsxfile)
for survey_id, survey_date in zip(survey_ids, survey_dates):
    print ("Starting on survey " + str(survey_id))
    df = pd.read_sql(sql, conn, 
                     #index_col="room_id",
                     params={"survey_id": survey_id}
                    )
    print ("To Excel on survey " + str(survey_id))
    df.to_excel(writer, sheet_name=str(survey_date))
print ("Saving")
writer.save()
