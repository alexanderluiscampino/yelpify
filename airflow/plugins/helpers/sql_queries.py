class SqlQueries:

    business_fact_create = ("""
        CREATE TABLE "business_fact" (
        "business_id" varchar PRIMARY KEY,
        "name" varchar,
        "categories" varchar,
        "review_count" bigint,
        "stars" count,
        "city_id" varchar,
        "address" varchar,
        "postal_code" varchar
        );
    """)

    city_fact_create = ("""
        CREATE TABLE "city_fact" (
        "city_id" varchar PRIMARY KEY,
        "state" varchar,
        "city" varchar
        );
    """)

    users_fact_create = ("""
        CREATE TABLE "users_fact" (
        "user_id" varchar PRIMARY KEY,
        "yelping_since" timestamp,
        "name" varchar,
        "average_stars" int,
        "review_count" bigint
        )
    """)

    review_dim_create = ("""
        CREATE TABLE "review_dim" (
        "review_id" varchar PRIMARY KEY,
        "review_date" timestamp,
        "business_id" varchar,
        "user_id" varchar
        );
    """)

    review_fact_create = ("""
        CREATE TABLE "review_fact" (
        "review_id" varchar PRIMARY KEY,
        "stars" int,
        "text" varchar
        );
    """)

    stock_fact_create = ("""
        CREATE TABLE "stock_fact" (
        "stock_id" varchar PRIMARY KEY,
        "business_name" varchar,
        "date" timestamp,
        "close_value" float
        );
    """)

    tip_fact_create = ("""
        CREATE TABLE "tip_fact" (
        "tip_id" varchar PRIMARY KEY,
        "business_id" varchar,
        "user_id" varchar,
        "text" varchar,
        "tip_date" timestamp,
        "compliment_count" bigint
        );
    """)

    review_stage_create = ("""
        CREATE TABLE "review_staging" (
        "business_id" varchar
        "cool" bigint,
        "funny" bigint,
        "review_id" varchar,
        "stars" double,
        "text" varchar,
        "useful" bigint,
        "user_id" string,
        "dt" varchar
        );
    """)
    business_stage_create = ("""
        CREATE TABLE "business_staging" (
        "business_id" varchar,
        "categories" varchar,
        "state" varchar,
        "city" varchar,
        "address" varchar,
        "postal_code" string,
        "review_count" bigint,
        "stars" double
        );
    """)
    tip_stage_create = ("""
        CREATE TABLE "tip_staging" (
        "business_id" varchar,
        "compliment_count" bigint,
        "text" varchar,
        "user_id" varchar,
        "dt" varchar
        );
    """)
    users_stage_create = ("""
        CREATE TABLE "users_staging" (
        "average_stars" varchar 
        "compliment_cool" bigint,
        "compliment_cute" bigint,
        "compliment_funny" bigint,
        "compliment_hot" bigint,
        "compliment_list" bigint,
        "compliment_more" bigint,
        "compliment_note" bigint,
        "compliment_photos" bigint,
        "compliment_plain" bigint,
        "compliment_profile" bigint,
        "compliment_writer" bigint,
        "cool" bigint,
        "elite" varchar,
        "fans" bigint,
        "friends" varchar,
        "funny" bigint,
        "name" varchar,
        "review_count" bigint,
        "useful" bigint,
        "user_id" varchar,
        "yelping_since" varchar
        );
    """)

    stock_stage_create = ("""
        CREATE TABLE "stock_staging" (
        "Date" varchar,
        "Open" double,
        "High" double,
        "Low" double,
        "Close" double,
        "Volume" bigint,
        "OpenInt" bigint
        );
    """)

    users_fact_table_insert = ("""
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

    business_fact_table_insert = ("""
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

    city_fact_table_insert = ("""
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

    review_dim_table_insert = ("""
        INSERT INTO review_dim (
            review_id,
            review_date,
            business_id,
            user_id
            )
        SELECT distinct
            review_id,
            CAST(dt as timestamp) AS review_date,
            business_id,
            user_id
        FROM review_staging
    """)

    review_fact_table_insert = ("""
        INSERT INTO review_fact (
            review_id,
            stars,
            text
            )
        SELECT distinct
            review_id,
            stars,
            text
        FROM review_staging
    """)

    tip_fact_table_insert = ("""
        INSERT INTO tip_fact (
            tip_id,
            business_id,
            user_id,
            text,
            tip_date,
            compliment_count
            )
        SELECT distinct
            md5(business_id || user_id || tip_date)  tip_id,
            business_id,
            user_id,
            text,
            CAST(dt as timestamp) AS tip_date,
            compliment_count
        FROM tip_staging
    """)

    stock_fact_table_insert = ("""
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
