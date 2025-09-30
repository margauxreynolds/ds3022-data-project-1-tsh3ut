import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/clean.log'
)
logger = logging.getLogger(__name__)

def clean_parquet_files():
    """
    Clean yellow and green trip data from raw data tables:
    - Remove any duplicate trips
    - Remove trips with 0 passengers
    - Remove trips with 0 miles in length
    - Remove trips longer than 100 miles in length
    - Remove trips longer than one day in length (86400 seconds)

    Creates cleaned tables: yellow_trips_2024 and green_trips_2024
    """
    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Yellow Trips Cleaning
        print("Cleaning yellow_trips_2024 table...")
        logger.info("Cleaning yellow_trips_2024 table...")

        raw_yellow_count = con.execute(f"""
            -- number of rows in table before cleaning
            SELECT COUNT(*) FROM yellow_trips_2024;
        """).fetchone()[0]

        con.execute(f"""
            -- Yellow Trips Cleaning
            DROP TABLE IF EXISTS yellow_trips_2024_clean;
            CREATE TABLE yellow_trips_2024_clean AS
            SELECT DISTINCT * 
            FROM yellow_trips_2024
            WHERE passenger_count > 0
                AND trip_distance > 0
                AND trip_distance <= 100
                AND date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) <= 86400;
            
            DROP TABLE yellow_trips_2024;
            ALTER TABLE yellow_trips_2024_clean RENAME TO yellow_trips_2024;
	    """)
        print("Cleaned yellow_trips_2024 table created.")
        logger.info("Cleaned yellow_trips_2024 table created.")

        y_rows = con.execute(f"""
            -- number of rows in cleaned table
            SELECT COUNT(*) FROM yellow_trips_2024;
        """).fetchone()[0]
        print(f"Cleaned yellow_trips_2024 rows: {y_rows:,}")
        print(f"Yellow rows removed during cleaning: {raw_yellow_count - y_rows:,}")
        logger.info(f"Yellow rows removed during cleaning: {raw_yellow_count - y_rows:,}")

        # Green Trips Cleaning
        print("\nCleaning green_trips_2024 table...")
        logger.info("Cleaning green_trips_2024 table...")

        raw_green_count = con.execute(f"""
            -- number of rows in table before cleaning
            SELECT COUNT(*) FROM green_trips_2024;
        """).fetchone()[0]

        con.execute(f"""
            -- Green Trips Cleaning
            DROP TABLE IF EXISTS green_trips_2024_clean;
            CREATE TABLE green_trips_2024_clean AS
            SELECT DISTINCT * 
            FROM green_trips_2024
            WHERE passenger_count > 0
                AND trip_distance > 0
                AND trip_distance <= 100
                AND date_diff('second', lpep_pickup_datetime, lpep_dropoff_datetime) <= 86400;

            DROP TABLE green_trips_2024;
            ALTER TABLE green_trips_2024_clean RENAME TO green_trips_2024;
	    """)
        print("Cleaned green_trips_2024 table created.")
        logger.info("Cleaned green_trips_2024 table created.")

        g_rows = con.execute(f"""
            -- number of rows in cleaned table
            SELECT COUNT(*) FROM green_trips_2024;
        """).fetchone()[0]
        print(f"Cleaned green_trips_2024 rows: {g_rows:,}")
        print(f"Green rows removed during cleaning: {raw_green_count - g_rows:,}")
        logger.info(f"Green rows removed during cleaning: {raw_green_count - g_rows:,}")

        # Cleaning verification 
        def cleaning_tests(table, pickup, dropoff):
            print(f"\nCleaning Tests for {table}:")
            logger.info(f"Cleaning Tests for {table}:")

            # Check for duplicate trips
            total = con.execute(f"""
                SELECT COUNT(*) FROM {table};
            """).fetchone()[0]
            distinct_count = con.execute(f"""
                SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {table});
            """).fetchone()[0]

            # duplicates = total - distinct(*)
            dupes = total - distinct_count
            print(f"Number of duplicate rows: {dupes}")
            logger.info(f"Number of duplicate rows: {dupes}")

            # Check for trips with 0 passengers
            zero_passengers = con.execute(f"""
                SELECT COUNT(*) FROM {table} WHERE passenger_count = 0;
            """).fetchone()[0]
            print(f"Number of trips with 0 passengers: {zero_passengers}")
            logger.info(f"Number of trips with 0 passengers: {zero_passengers}")

            # Check for trips <= 0 miles
            zero_trip = con.execute(f"""
                SELECT COUNT(*) FROM {table} WHERE trip_distance <= 0;
            """).fetchone()[0]
            print(f"Number of trips 0 miles long: {zero_trip}")
            logger.info(f"Number of trips 0 miles long: {zero_trip}")

            # Check for trip distance > 100 miles
            long_distance = con.execute(f"""
                SELECT COUNT(*) FROM {table} WHERE trip_distance > 100;
            """).fetchone()[0]
            print(f"Number of trips with > 100 miles: {long_distance}")
            logger.info(f"Number of trips with > 100 miles: {long_distance}")

            # Check for trip duration > 24 hours (86400 seconds)
            long_duration = con.execute(f"""
                SELECT COUNT(*) FROM {table} 
                WHERE date_diff('second', {pickup}, {dropoff}) > 86400;
            """).fetchone()[0]
            print(f"Number of trips lasting longer than one day: {long_duration}")
            logger.info(f"Number of trips lasting longer than one day: {long_duration}")

        cleaning_tests("yellow_trips_2024", "tpep_pickup_datetime", "tpep_dropoff_datetime")
        cleaning_tests("green_trips_2024", "lpep_pickup_datetime", "lpep_dropoff_datetime")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_parquet_files()
    print("Data cleaning complete.")
    logger.info("Data cleaning complete.")