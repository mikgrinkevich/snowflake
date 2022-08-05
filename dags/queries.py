create_db_objects_query = """
    use database REVIEWS;
    create or replace table RAW_TABLE (
        _ID VARCHAR,
        IOS_APP_ID NUMBER,
        TITLE VARCHAR,
        DEVELOPER_NAME VARCHAR,
        DEVELOPER_IOS_ID FLOAT,
        IOS_STORE_URL VARCHAR,
        SELLER_OFFICIAL_WEBSITE VARCHAR,
        AGE_RATING VARCHAR,
        TOTAL_AVERAGE_RATING FLOAT,
        TOTAL_NUMBER_OF_RATINGS FLOAT,
        AVERAGE_RATING_FOR_VERSION FLOAT,
        NUMBER_OF_RATINGS_FOR_VERSION NUMBER,
        Original_Release_Date VARCHAR,
        CURRENT_VERSION_RELEASE_DATE VARCHAR,
        PRICE_USD FLOAT,
        PRIMARY_GENRE VARCHAR,
        ALL_GENRES VARCHAR,
        LANGUAGES VARCHAR,
        DESCRIPTION VARCHAR
        );

    create or replace table stage_table like RAW_TABLE;
    create or replace table master_table like RAW_TABLE;
    create or replace stream raw_stream on table RAW_TABLE;
    create or replace stream stage_stream on table stage_table;"""

columns ="""select _ID, IOS_APP_ID, TITLE, DEVELOPER_NAME, DEVELOPER_IOS_ID,
    IOS_STORE_URL, SELLER_OFFICIAL_WEBSITE, AGE_RATING, TOTAL_AVERAGE_RATING,
    TOTAL_NUMBER_OF_RATINGS, AVERAGE_RATING_FOR_VERSION, NUMBER_OF_RATINGS_FOR_VERSION,
    ORIGINAL_RELEASE_DATE, CURRENT_VERSION_RELEASE_DATE, PRICE_USD,PRIMARY_GENRE, ALL_GENRES, LANGUAGES, DESCRIPTION"""

# columns = """select COLUMN_NAME from information_schema.columns where table_name='RAW_TABLE';"""
