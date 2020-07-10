from enum import Enum, unique


@unique
class SqlQueries(Enum):

    setup_foreign_keys = ("""
    ALTER TABLE "tip_fact" ADD FOREIGN KEY ("business_id") REFERENCES "business_fact" ("business_id");
    ALTER TABLE "tip_fact" ADD FOREIGN KEY ("user_id") REFERENCES "users_fact" ("user_id");
    ALTER TABLE "business_fact" ADD FOREIGN KEY ("city_id") REFERENCES "city_fact" ("city_id");
    ALTER TABLE "review_dim" ADD FOREIGN KEY ("business_id") REFERENCES "business_fact" ("business_id");
    ALTER TABLE "review_dim" ADD FOREIGN KEY ("user_id") REFERENCES "users_fact" ("user_id");
    ALTER TABLE "review_fact" ADD FOREIGN KEY ("review_id") REFERENCES "review_dim" ("review_id");
    ALTER TABLE "stock_fact" ADD FOREIGN KEY ("business_name") REFERENCES "business_fact" ("name");
    """)

    business_fact_create = ("""
        DROP TABLE IF EXISTS business_fact;
        CREATE TABLE IF NOT EXISTS "business_fact" (
        "business_id" varchar PRIMARY KEY,
        "name" varchar,
        "categories" varchar(MAX),
        "review_count" bigint,
        "stars" float,
        "city_id" varchar,
        "address" varchar(MAX),
        "postal_code" varchar
        );

    """)

    city_fact_create = ("""
        DROP TABLE IF EXISTS city_fact;
        CREATE TABLE IF NOT EXISTS "city_fact" (
        "city_id" varchar PRIMARY KEY,
        "state" varchar,
        "city" varchar
        );

    """)

    users_fact_create = ("""
        DROP TABLE IF EXISTS users_fact;
        CREATE TABLE IF NOT EXISTS "users_fact" (
        "user_id" varchar(MAX) PRIMARY KEY,
        "yelping_since" timestamp,
        "name" varchar,
        "average_stars" float,
        "review_count" bigint
        );

    """)

    review_dim_create = ("""
        DROP TABLE IF EXISTS review_dim;
        CREATE TABLE IF NOT EXISTS "review_dim" (
        "review_id" varchar(MAX) PRIMARY KEY,
        "review_date" timestamp,
        "business_id" varchar(MAX),
        "user_id" varchar
        );

    """)

    review_fact_create = ("""
        DROP TABLE IF EXISTS review_fact;
        CREATE TABLE IF NOT EXISTS "review_fact" (
        "review_id" varchar PRIMARY KEY,
        "stars" float,
        "text" varchar(MAX)
        );

    """)

    stock_fact_create = ("""
        DROP TABLE IF EXISTS stock_fact;
        CREATE TABLE IF NOT EXISTS "stock_fact" (
        "stock_id" varchar PRIMARY KEY,
        "business_name" varchar,
        "date" timestamp,
        "close_value" float
        );
    """)

    tip_fact_create = ("""
        DROP TABLE IF EXISTS tip_fact;
        CREATE TABLE IF NOT EXISTS "tip_fact" (
        "tip_id" varchar(MAX) PRIMARY KEY,
        "business_id" varchar(MAX),
        "user_id" varchar(MAX),
        "text" varchar(MAX),
        "tip_date" timestamp,
        "compliment_count" bigint
        );

    """)

    review_stage_create = ("""
        DROP TABLE IF EXISTS review_staging;
        CREATE TABLE IF NOT EXISTS "review_staging" (
        "business_id" varchar(MAX),
        "date" varchar,
        "review_id" varchar,
        "stars" varchar,
        "text" varchar(MAX),
        "user_id" varchar(MAX)
        );
    """)
    business_stage_create = ("""
        DROP TABLE IF EXISTS business_staging;
        CREATE TABLE IF NOT EXISTS "business_staging" (
        "business_id" varchar(MAX),
        "name" varchar,
        "categories" varchar(MAX),
        "state" varchar,
        "city" varchar,
        "address" varchar(MAX),
        "postal_code" varchar,
        "review_count" bigint,
        "stars" float
        );
    """)
    tip_stage_create = ("""
        DROP TABLE IF EXISTS tip_staging;
        CREATE TABLE IF NOT EXISTS "tip_staging" (
        "business_id" varchar(MAX),
        "compliment_count" bigint,
        "date" varchar,
        "text" varchar(MAX),
        "user_id" varchar(MAX)
        );
    """)
    users_stage_create = ("""
        DROP TABLE IF EXISTS users_staging;
        CREATE TABLE IF NOT EXISTS "users_staging" (
        "average_stars" float,
        "name" varchar,
        "review_count" bigint,
        "user_id" varchar,
        "yelping_since" varchar
        );
    """)

    stock_stage_create = ("""
        DROP TABLE IF EXISTS stock_staging;
        CREATE TABLE IF NOT EXISTS "stock_staging" (
        "Date" varchar,
        "Open" float,
        "High" float,
        "Low" float,
        "Close" float,
        "Volume" bigint,
        "OpenInt" bigint
        );
    """)

    users_fact_insert = ("""
        INSERT INTO users_fact (
            user_id,
            yelping_since,
            name,
            average_stars,
            review_count
            )
        SELECT distinct
            user_id, 
            CAST(yelping_since as timestamp) AS yelping_since,
            name, 
            average_stars, 
            review_count
        FROM users_staging
    """)

    business_fact_insert = ("""
        INSERT INTO business_fact (
            business_id,
            name,
            categories,
            review_count,
            stars,
            city_id,
            address,
            postal_code
            )
        SELECT distinct 
            business_id,
            name,
            categories,
            review_count,
            stars,
            b.city_id,
            address,
            postal_code
        FROM business_staging a
        LEFT JOIN city_fact b ON a.city = b.city AND a.state = b.state
    """)

    city_fact_insert = ("""
        INSERT INTO city_fact (
            city_id,
            state,
            city
            )
        SELECT distinct
            md5(state || city) city_id,
            state,
            city
        FROM business_staging
    """)

    review_dim_insert = ("""
        INSERT INTO review_dim (
            review_id,
            review_date,
            business_id,
            user_id
            )
        SELECT distinct
            review_id,
            CAST(date as timestamp) AS review_date,
            business_id,
            user_id
        FROM review_staging
    """)

    review_fact_insert = ("""
        INSERT INTO review_fact (
            review_id,
            stars,
            text
            )
        SELECT distinct
            review_id,
            CAST(stars AS BIGINT) AS stars,
            text
        FROM review_staging
    """)

    tip_fact_insert = ("""
        INSERT INTO tip_fact (
            tip_id,
            business_id,
            user_id,
            text,
            tip_date,
            compliment_count
            )
        SELECT distinct
            md5(business_id || user_id || date)  tip_id,
            business_id,
            user_id,
            text,
            CAST(date as timestamp) AS tip_date,
            compliment_count
        FROM tip_staging
    """)

    stock_fact_insert = ("""
        INSERT INTO stock_fact (
            stock_id,
            business_name,
            date,
            close_value
            )
        SELECT distinct
            md5('cmg' || date ) stock_id,
            'chipotle' AS business_name,
            Date,
            Close
        FROM stock_staging
    """)
