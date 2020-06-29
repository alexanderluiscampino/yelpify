# Yelpify
The goal of this project is to create a data pipeline for Yelp business review data and stock market values. Potentially, once the data is in its final format, it can be used to derive relationships between how much people use they app, their sentiment on reviews and the stock price of yelp and others businesses that yelp users review ( Chipotle, McDonalds, ShakeShack, etc..)

The following techologies shall be used for this pipeline:

- Storage: AWS S3 for file object storage due to his high availbility, simple access, low cost for massive amounts of that and almost infinite scalability in terms of storage space. Storing 1gb or 10tb of data only increases the cost of application but not the performance of the underlying system. This is a perfect solution for ever increasing datasets with frequent access of from/to data pipelines
- Orchestration: Airflow will be used to orchestrate data pipelines runs. Thus pipeline can be run on a schedule, 1x a day, 1x a week, 1x month, etc. The user will have the possibility to choose the schedule to run and its frequency, without any overhead conflict.
- Data Warehouse: AWS Redshift serves as a perfect solution for this, since it is designed for massive amounts of data processing and transformation. Normal SQL will be used to apply transformation to the raw data and store it in a Redshift Cluster. This cluster can scaled accordingly to the need of the users. CPUs, memory and storage space can be added accordingly to the demand of the end users. The cluster can be scaled up in terms of resources to accomodate more users of the app, to alows for a continous and smooth experience, where queries run at a good speed. The final snowflake data schema can be used in any report/visualization or to feed Machine Learning models. This schema is easily digestable and its connection are easily understandable.
- Data Processing: PySpark will be used for some of the initial data processing. Since the data arrives from difference sources and in massive datasets, it will be useful for the initial ingestion and data partitioning to be performed used Spark technologies. This can be done in a EMR cluster, which can easily be scaled up/down according to the needs (dataset size and how fast the pipeline should run)



# Datasets
## Yelp Customer Data
These datasets can be found in [Kaggle](https://www.kaggle.com/yelp-dataset/yelp-dataset). It comes in `JSON` format and the following files can be found:
- `yelp_academic_dataset_checkin`
- `yelp_academic_dataset_user`
- `yelp_academic_dataset_business`
- `yelp_academic_dataset_tip`
- `yelp_academic_dataset_review`

### Checkin

### User

### Business
* address: string (nullable = true)
 * attributes: struct (nullable = true)
   * AcceptsInsurance: string (nullable = true)
   * AgesAllowed: string (nullable = true)
   * Alcohol: string (nullable = true)
   * Ambience: string (nullable = true)
   * BYOB: string (nullable = true)
   * BYOBCorkage: string (nullable = true)
   * BestNights: string (nullable = true)
   * BikeParking: string (nullable = true)
   * BusinessAcceptsBitcoin: string (nullable = true)
   * BusinessAcceptsCreditCards: string (nullable = true)
   * BusinessParking: string (nullable = true)
   * ByAppointmentOnly: string (nullable = true)
   * Caters: string (nullable = true)
   * CoatCheck: string (nullable = true)
   * Corkage: string (nullable = true)
   * DietaryRestrictions: string (nullable = true)
   * DogsAllowed: string (nullable = true)
   * DriveThru: string (nullable = true)
   * GoodForDancing: string (nullable = true)
   * GoodForKids: string (nullable = true)
   * GoodForMeal: string (nullable = true)
   * HairSpecializesIn: string (nullable = true)
   * HappyHour: string (nullable = true)
   * HasTV: string (nullable = true)
   * Music: string (nullable = true)
   * NoiseLevel: string (nullable = true)
   * Open24Hours: string (nullable = true)
   * OutdoorSeating: string (nullable = true)
   * RestaurantsAttire: string (nullable = true)
   * RestaurantsCounterService: string (nullable = true)
   * RestaurantsDelivery: string (nullable = true)
   * RestaurantsGoodForGroups: string (nullable = true)
   * RestaurantsPriceRange2: string (nullable = true)
   * RestaurantsReservations: string (nullable = true)
   * RestaurantsTableService: string (nullable = true)
   * RestaurantsTakeOut: string (nullable = true)
   * Smoking: string (nullable = true)
   * WheelchairAccessible: string (nullable = true)
   * WiFi: string (nullable = true)git remote add origin
 * business_id: string (nullable = true)
 * categories: string (nullable = true)
 * city: string (nullable = true)
 * hours: struct (nullable = true)
   * Friday: string (nullable = true)
   * Monday: string (nullable = true)
   * Saturday: string (nullable = true)
   * Sunday: string (nullable = true)
   * Thursday: string (nullable = true)
   * Tuesday: string (nullable = true)
   * Wednesday: string (nullable = true)
 * is_open: long (nullable = true)
 * latitude: double (nullable = true)
 * longitude: double (nullable = true)
 * name: string (nullable = true)
 * postal_code: string (nullable = true)
 * review_count: long (nullable = true)
 * stars: double (nullable = true)
 * state: string (nullable = true)


### Tip

### Review