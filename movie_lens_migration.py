import psycopg2
import csv

link = psycopg2.connect("dbname=movie_lens user=movie_lens")
cursor = link.cursor()

cursor.execute("DROP TABLE IF EXISTS movie_data;")
create_table_command = """
CREATE TABLE movie_data (
    userid SERIAL PRIMARY KEY NOT NULL,
    age INT,
    gender VARCHAR(1),
    occupation VARCHAR(30),
    zipcode VARCHAR(10)
);
"""

cursor.execute(create_table_command)
with open("mov_user.csv") as open_file:
    contents = csv.reader(open_file, delimiter="|")
    for row in contents:
        cursor.execute("INSERT INTO movie_data VALUES(%s, %s, %s, %s, %s);",
                       (row[0], row[1], row[2], row[3], row[4]))
link.commit()

cursor.execute("DROP TABLE IF EXISTS movie_lens;")

create_table = """
CREATE TABLE movie_lens (
    movie_id int,
    movie_name varchar(500),
    year_video_released varchar(20),
    empty_slot varchar(10),
    website_url varchar(200),
    the_unknown int,
    action int,
    adventure int,
    animation int,
    childrens int,
    comedy int,
    crime int,
    documentary int,
    drama int,
    fantasy int,
    film_noir int,
    horror int,
    musical int,
    mystery int,
    romance int,
    sci_fi int,
    thriller int,
    war int,
    western int
);
"""

cursor.execute(create_table)
with open("item.csv", encoding="latin1") as open_file:
    movie_details = csv.reader(open_file, delimiter="|")

    for row in movie_details:
        print(row)
        cursor.execute("INSERT INTO movie_lens values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",
            (row[:]))
link.commit()
cursor.close()
link.close()
