#!/usr/bin/python3

# coding: utf-8

# # Load Summary Statistics
# 
# Utility notebook to load data from room into survey_snapshot and
# survey_interval, for quick aggregate queries.

# In[1]:


import psycopg2 as pg
_conn = None

def connect():
    try:
        global _conn
        if _conn is None or _conn.closed:
            _conn = pg.connect(
                user="postgres",
                password="time2PSQLsleep",
                host='airbnb.cphovrwl1wxm.us-east-1.rds.amazonaws.com',
                port=5432,
                database='airbnb')
            _conn.set_client_encoding('UTF8')
            return
        _conn
    except: 
        print("Failed to connect to database")
        
        connection = connect()
        sql_abbrevs = """
        select distinct abbreviation
        from search_area where
        abbreviation is not null
        """
        cursor
        =
        connection.cursor()
        cursor.execute(sql_abbrevs)
        abbreviation_list
        =
        []
        for
        rs
        in
        cursor.fetchall():
            abbreviation_list.append(rs[0])
            abbreviation_list
            =
            sorted(abbreviation_list)
            #print(abbreviation_list)


# In[2]:


# abbreviation_list
# =
# ['kw']
for
abbreviation
in
abbreviation_list:
    try:
        print(abbreviation)
        sql = """
        insert into survey_snapshot (
        survey_id, city, status, survey_date, listings, listings_active, listings_eh,
        listings_pr, listings_sr, reviews_mean, rating_mean, price_mean)
        select s.survey_id survey_id, sa.name as city,
        s.status status, s.survey_date survey_date, count(*) listings,
        count(*) FILTER (where reviews > 0) listings_active,
        count(*) FILTER (where room_type = 'Entire home/apt') listings_eh,
        count(*) FILTER (where room_type = 'Private room') listings_pr,
        count(*) FILTER (where room_type = 'Shared room') listings_sr,
        avg(reviews) reviews_mean,
        avg(overall_satisfaction) FILTER (where overall_satisfaction > 0) rating_mean,
        avg(price) price_mean from listing_{} r 
        join survey s
        on r.survey_id = s.survey_id
        join search_area sa
        on s.search_area_id = sa.search_area_id
        group by s.survey_id, s.status, s.survey_date, sa.name
        order by s.survey_date
        """.format(abbreviation)
        connection = connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        cursor.close()
        connection.commit()
    except Exception as ex:
        print(ex)
        connection.close() 


# In[5]:


# host
# values
for abbreviation in abbreviation_list:
    try:
        sql = """SELECT
        survey_id, count(host_id) hosts,
        count(host_id) filter(where listings = 1) hosts_single,
        count(host_id) filter(where listings > 1) hosts_multiple,
        sum(listings) filter(where listings = 1) listings_single,
        sum(listings) filter(where listings > 1) listings_multiple,
        sum(reviews) filter(where listings = 1) reviews_single,
        sum(reviews) filter(where listings > 1) reviews_multiple
        from
            (SELECT r.survey_id as survey_id,
            host_id,
            count(*) AS listings,
            sum(r.reviews) AS reviews,
            sum(r.price * r.reviews::double precision) AS relative_income
            FROM
            listing_{} as r, survey s, search_area sa
            WHERE r.survey_id = s.survey_id 
            AND s.search_area_id = sa.search_area_id 
            GROUP BY r.survey_id, r.host_id) T
        group by survey_id
        """.format(abbreviation)
        connection = connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        sql_update = """
        UPDATE survey_snapshot
        SET hosts = %(hosts)s, hosts_multiple = %(hosts_multiple)s,
        listings_multiple = %(listings_multiple)s, reviews_multiple = %(reviews_multiple)s
        WHERE survey_id = %(survey_id)s
        """
        for row in cursor.fetchall():
            survey_id = row[0]
            hosts = row[1]
            hosts_single = row[2]
            hosts_multiple = row[3]
            listings_single = row[4]
            listings_multiple = row[5]
            reviews_single = row[6]
            reviews_multiple = row[7]
            cursor_update = connection.cursor()
            cursor_update.execute(sql_update,
                                  { "hosts": hosts,
                                      "hosts_single": hosts_single,
                                      "hosts_multiple": hosts_multiple,
                                      "listings_single": listings_single,
                                      "listings_multiple": listings_multiple,
                                      "reviews_single": reviews_single,
                                      "reviews_multiple": reviews_multiple,
                                      "survey_id": survey_id
                                  })
            cursor_update.close()
            cursor.close()
            connection.commit()
            print(abbreviation)
    except Exception as ex:
        print(ex)
        connection.close()       


# In[
# ]:


# entire
# home
# numbers
for abbreviation in abbreviation_list:
    try:       
        sql_eh = """SELECT
        survey_id, count(host_id) hosts_eh,
        count(host_id) filter(where listings > 1) hosts_multiple_eh,
        sum(listings) filter(where listings > 1) listings_multiple_eh,
        sum(reviews) filter(where listings > 1) reviews_multiple_eh
        from
        ( SELECT r.survey_id as survey_id,
        host_id, count(*) AS listings, sum(r.reviews) AS reviews,
        sum(r.price * r.reviews::double precision) AS relative_income
        FROM listing_{} as r, survey s, search_area sa
        WHERE r.survey_id = s.survey_id 
        AND room_type = 'Entire home/apt'
        AND s.search_area_id = sa.search_area_id 
        GROUP BY r.survey_id, r.host_id) T
        group by survey_id
        """.format(abbreviation)
        connection = connect()
        cursor = connection.cursor()
        cursor.execute(sql_eh)
        sql_update = """
        UPDATE survey_snapshot
        SET hosts_eh = %(hosts_eh)s,
        hosts_multiple_eh = %(hosts_multiple_eh)s,
        listings_multiple_eh = %(listings_multiple_eh)s,
        reviews_multiple_eh = %(reviews_multiple_eh)s
        WHERE survey_id = %(survey_id)s
        """
        for row in cursor.fetchall():
            survey_id = row[0]
            hosts_eh = row[1]
            hosts_multiple_eh = row[2]
            listings_multiple_eh = row[3]
            reviews_multiple_eh = row[4]
            cursor_update = connection.cursor()
            cursor_update.execute(sql_update,
                                  {
                                      "hosts_eh": hosts_eh,
                                      "hosts_multiple_eh": hosts_multiple_eh,
                                      "listings_multiple_eh": listings_multiple_eh,
                                      "reviews_multiple_eh": reviews_multiple_eh,
                                      "survey_id": survey_id
                                  })
            cursor_update.close()
            cursor.close()
        connection.commit()
        connection.close()
        print(abbreviation)
    except Exception as ex:
        print(ex)
        connection.close()


# #
# Load
# interval
# statistics
#

# In[
# ]:


# abbreviation_list
# =
# ['kw']


# In[
# ]:


for abbreviation in abbreviation_list:
    try:
        sql_temp_table =
        """ create temporary table panel
        as
        select room_id, r.survey_id survey_id,
        -- the more recent survey
        survey_date,
        (survey_date - nth_value(survey_date, 1) over w) as days_interval,
        row_number() over w as row_number,   
        sa.name as city,
        room_type, host_id,
        nth_value(reviews, 1) over w as reviews_one, 
        nth_value(reviews, 2) over w as reviews_two,
        avg(price) over w as price,
        nth_value(overall_satisfaction, 1) over w as rating_one,
        nth_value(overall_satisfaction, 2) over w as rating_two,
        count(*) over w as endpoints
        from
        listing_{abbreviation} r join survey s
        on r.survey_id = s.survey_id
        join search_area sa
        on s.search_area_id = sa.search_area_id
        window w as
        (partition by room_id
        order by r.survey_id
        rows between 1 preceding and current row);
        """.format(abbreviation=abbreviation)
        connection
        =
        pg.connect(
            user="postgres",
            password="time2PSQLsleep",
            host='airbnb.cphovrwl1wxm.us-east-1.rds.amazonaws.com',
            port=5432,
            database='airbnb')
        connection.set_client_encoding('UTF8')
        cursor
        =
        connection.cursor()
        cursor.execute(sql_temp_table)
        cursor.close()
        connection.commit()
        sql_load
        =
        """
        insert into survey_interval ( survey_id, survey_date, city,
        interval, listings, listings_new, reviews_monthly_total)
        select survey_id, survey_date, city,
        min(days_interval) filter (where days_interval > 0) as interval,
        sum(interval_listings_all) as listings, 
        sum(interval_listings_one) as listings_new,
        sum(interval_reviews_mean_two * interval_listings_two * 30.0 / days_interval) filter (where days_interval > 0) as reviews_monthly_total
        from
        (
        select survey_id, survey_date, days_interval,
        city, count(*) as interval_listings_all,
        count(*) filter (where endpoints = 2) as interval_listings_two,
        count(*) filter (where endpoints = 1) as interval_listings_one,
        avg(reviews_two - reviews_one) filter (where endpoints = 2) as interval_reviews_mean_two,
        avg(reviews_one) filter (where endpoints = 1) as interval_reviews_mean_one
        from panel
        group by survey_id, survey_date, days_interval, city
        order by survey_date, days_interval 
        ) t
        group by survey_id, survey_date, city
        order by survey_id
        """
        cursor = connection.cursor()
        cursor.execute(sql_load)
        cursor.close()
        connection.commit()
    except Exception as ex:
        print(ex)


# In[
# ]:


# Stage
# 2:
    # update
    # to
    # fill
    # in
    # listings_gone

for abbreviation in abbreviation_list:  
    try:
        sql_update_query = """
        select survey_id, (listings_previous + listings_new - listings) as listings_gone
        from (
            select survey_id, nth_value (listings, 1) over w as listings_previous,
            listings_new, listings,
            count(*) over w as endpoints from survey_interval
            window w as (
                partition by city
                order by survey_id
                rows between 1 preceding and current row)
            ) as t
        where endpoints = 2
        """
        connection = pg.connect(
            user="postgres",
            password="time2PSQLsleep",
            host='airbnb.cphovrwl1wxm.us-east-1.rds.amazonaws.com',
            port=5432,
            database='airbnb')
        connection.set_client_encoding('UTF8')
        cursor = connection.cursor()
        cursor.execute(sql_update_query)
        sql_update = """ UPDATE survey_interval
        SET listings_gone = %(listings_gone)s
        WHERE survey_id = %(survey_id)s
        """
        for row in cursor.fetchall():
            survey_id = row[0]
            listings_gone = row[1]
            cursor_update = connection.cursor()
            cursor_update.execute(sql_update,
                                  {
                                      "listings_gone": listings_gone,
                                      "survey_id": survey_id
                                  })
            cursor_update.close()
        cursor.close()
        connection.commit()
        connection.close()
    except Exception as ex:
        print(ex)
