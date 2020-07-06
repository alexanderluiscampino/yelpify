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

CREATE TABLE "city_fact" (
  "city_id" varchar PRIMARY KEY,
  "state" varchar,
  "city" varchar
);

CREATE TABLE "users_fact" (
  "user_id" varchar PRIMARY KEY,
  "yelping_since" timestamp,
  "name" varchar,
  "average_stars" int,
  "review_count" bigint
);

CREATE TABLE "review_dim" (
  "review_id" varchar PRIMARY KEY,
  "review_date" timestamp,
  "business_id" varchar,
  "user_id" varchar
);

CREATE TABLE "review_fact" (
  "review_id" varchar PRIMARY KEY,
  "stars" int,
  "text" varchar
);

CREATE TABLE "stock_fact" (
  "stock_id" varchar PRIMARY KEY,
  "business_name" varchar,
  "date" timestamp,
  "close_value" float
);

CREATE TABLE "tip_fact" (
  "tip_id" varchar PRIMARY KEY,
  "business_id" varchar,
  "user_id" varchar,
  "text" varchar,
  "tip_date" timestamp,
  "compliment_count" bigint
);

ALTER TABLE "tip_fact" ADD FOREIGN KEY ("business_id") REFERENCES "business_fact" ("business_id");

ALTER TABLE "tip_fact" ADD FOREIGN KEY ("user_id") REFERENCES "users_fact" ("user_id");

ALTER TABLE "business_fact" ADD FOREIGN KEY ("city_id") REFERENCES "city_fact" ("city_id");

ALTER TABLE "review_dim" ADD FOREIGN KEY ("business_id") REFERENCES "business_fact" ("business_id");

ALTER TABLE "review_dim" ADD FOREIGN KEY ("user_id") REFERENCES "users_fact" ("user_id");

ALTER TABLE "review_fact" ADD FOREIGN KEY ("review_id") REFERENCES "review_dim" ("review_id");

ALTER TABLE "stock_fact" ADD FOREIGN KEY ("business_name") REFERENCES "business_fact" ("name");

