#!/usr/bin/python3
# ============================================================================
# Manage files in S3
# ============================================================================
import boto3
import os
import pandas as pd
import logging
import zipfile
from airbnb_config import ABConfig

AWS_S3_BUCKET = "tomslee-airbnb-data-2"
START_DATE = '2013-05-02'
# Set up logging
LOG_FORMAT = '%(levelname)-8s%(message)s'
LOG_LEVEL = logging.INFO
logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)


def surveys(ab_config):
    sql_survey_ids = """
        select survey_id, sa.name city, sa.abbreviation city_abbrev, survey_date, comment
        from survey s, search_area sa
        where s.search_area_id = sa.search_area_id
        and sa.abbreviation is not null
        and sa.bb_n_lat is not null
        and s.status = 1
        order by 2, 1
    """
    conn = ab_config.connect()
    cur = conn.cursor()
    cur.execute(sql_survey_ids)
    rs = cur.fetchall()
    logging.info("Found {surveys} surveys.".format(surveys=len(rs)))
    cur.close()
    conn.close()
    return(rs)


def get_city_view(ab_config, city, abbrev):
    try:
        view_name = "listing_" + abbrev
        sql = "select viewname from pg_views where viewname = '{}'".format(view_name)
        conn = ab_config.connect()
        cur = conn.cursor()
        cur.execute(sql)
        view = cur.fetchone()[0]
        cur.close()
        logging.debug("Found view for city {0}: {1}".format(city, view_name))
        return view
    except:
        logging.debug("No view for city {0}".format(city))
        return None


def city_data(ab_config, city, city_view, survey_id):
    # sql = """
        # select room_id, host_id, room_type,
            # borough, neighborhood,
            # reviews, overall_satisfaction,
            # accommodates, bedrooms,
            # price, minstay,
            # latitude, longitude,
            # last_modified
        # from {city_view}
        # where survey_id = %(survey_id)s
        # """.format(city_view=city_view)
    sql = """
        select *
        from {city_view}
        where survey_id = %(survey_id)s
        """.format(city_view=city_view)

    conn = ab_config.connect()
    df = pd.read_sql(sql, conn,
                     index_col="room_id",
                     params={"survey_id": survey_id}
                     )
    return df


def cities(ab_config, survey_list):
    city_views = {}
    logging.info("-" * 70)
    logging.info("Querying database for cities...")
    for survey in survey_list:
        (survey_id, city, city_abbrev, survey_date, comment) = survey
        if city in city_views:
            logging.debug("View for {0} is known: {1}".format(city, city_views[city]))
        else:
            city_view = get_city_view(ab_config, city, city_abbrev)
            if city_view:
                city_views[city] = city_view
                logging.debug("Found view for {0}: {1}".format(city, city_views[city]))
            else:
                continue
    return city_views


def write_csv_files(ab_config, survey_list, city_views, s3_dir):
    survey_counts = {}
    logging.info("-" * 70)
    logging.info("Querying database and writing csv files...")
    for survey in survey_list:
        (survey_id, city, city_abbrev, survey_date, comment) = survey
        if city not in city_views:
            continue
        city_view = city_views[city]
        city_bar = city.replace(" ", "_").lower()
        path = os.path.join(s3_dir, city_bar)
        csv_file = ("tomslee_airbnb_{city}_{survey_id:0>4}_{survey_date}.csv"
                    ).format(city=city_bar, survey_id=survey_id, survey_date=survey_date)
        csv_full_file_path = os.path.join(path, csv_file)
        # Only get the data if we don't already have it
        if os.path.isfile(csv_full_file_path):
            logging.info("File already exists: {csv_file}. Skipping...".format(csv_file=csv_full_file_path))
            df_data = pd.read_csv(csv_full_file_path, index_col="room_id", encoding="utf-8")
            survey_counts[survey_id] = len(df_data)
        else:
            df_data = city_data(ab_config, city, city_view, survey_id)
            if len(df_data) > 0:
                survey_counts[survey_id] = len(df_data)
                if not os.path.exists(path):
                    os.makedirs(path)
                logging.info("Writing {listings:>6} listings to {csv_file}..."
                            .format(listings=survey_counts[survey_id], csv_file=csv_full_file_path))
                df_data.to_csv(csv_full_file_path, encoding="utf-8")
    return survey_counts


def write_html_file(survey_list, city_views, survey_counts):
    """
    The HTML file contains a block of HTML that has descriptions of and a link
    to each zip file. I manually paste the file into the website for users.
    """
    logging.info("-" * 70)
    logging.info("Writing HTML list of links...")
    html = "<dl>\n"
    for city in sorted(city_views):
        city_bar = city.replace(" ", "_").lower()
        s = "<dt>{city}</dt>\n".format(city=city)
        s += "<dd><p>Survey dates: "
        for survey in survey_list:
            (survey_id, survey_city, city_abbrev, survey_date, comment) = survey
            survey_date = survey[3]
            if (survey_city == city) and (survey_id in survey_counts):
                s += "{survey_date} ({survey_count} listings), ".format(
                    survey_date=survey_date, survey_count=survey_counts[survey_id])
        s = s[:-2]
        s += "</p>\n"
        s += "<p><a href=\"https://s3.amazonaws.com/{bucket}/{city_bar}.zip\">Download zip file</a></p>\n".format(
            city_bar=city_bar, bucket=AWS_S3_BUCKET)
        s += "</dd>\n"
        html += s
        logging.info("City {0} linked.".format(city))
    html += "</dl>"
    f1 = open('city_list.html', 'w')
    f1.write(html)
    f1.close()


def zip_csv_files(city_views, s3_dir):
    logging.info("-" * 70)
    logging.info("Zipping data files...")
    for city in city_views:
        try:
            city_bar = city.replace(" ", "_").lower()
            csv_path = os.path.join(s3_dir, city_bar)
            zip_file = os.path.join(s3_dir, city_bar + ".zip")
            csv_files = [f for f in os.listdir(csv_path) if os.path.isfile(os.path.join(csv_path, f))]
            with zipfile.ZipFile(zip_file, 'w') as city_zip_file:
                for csv_file in csv_files:
                    city_zip_file.write(os.path.join(csv_path, csv_file))
            logging.info("\tCity {0} zipped.".format(city_bar))
        except:
            continue


def upload_zip_files(city_views, s3_dir):
    logging.info("-" * 70)
    logging.info("Uploading zip files...")
    s3 = boto3.resource('s3')
    logging.info("Connected to S3...")
    for city in city_views:
        city_bar = city.replace(" ", "_").lower()
        zip_file = os.path.join(s3_dir, city_bar + ".zip")
        if os.path.isfile(zip_file):
            key = city_bar + ".zip"
            s3.Object(AWS_S3_BUCKET, key).put(Body=open(zip_file, 'rb'))
            s3.Object(AWS_S3_BUCKET, key).Acl().put(ACL='public-read')
            # logging.info("\tCity {0} uploaded.".format(city_bar))
            logging.info("\tCity {0} uploaded.".format(zip_file))


def main():
    ab_config = ABConfig()
    survey_list = surveys(ab_config)
    city_views = cities(ab_config, survey_list)
    logging.debug(city_views)
    s3_dir = "s3_files"
    survey_counts = write_csv_files(ab_config, survey_list, city_views, s3_dir)
    write_html_file(survey_list, city_views, survey_counts)
    zip_csv_files(city_views, s3_dir)
    upload_zip_files(city_views, s3_dir)


if __name__ == "__main__":
    main()
