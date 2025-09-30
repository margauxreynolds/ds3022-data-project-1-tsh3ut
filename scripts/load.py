import duckdb
import os
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/load.log'
)
logger = logging.getLogger(__name__)

def load_parquet_files():
    """
    Load yellow and green trip data from 2024 monthly parquet files into DuckDB tables:
    - Load vehicle_emissions.csv into a DuckDB table named vehicle_emissions
    - Load all 12 months of yellow taxi trip data for 2024 into a single DuckDB table named yellow_trips_2024
    - Load all 12 months of green taxi trip data for 2024 into a single DuckDB table named green_trips_2024
    Creates 3 tables: vehicle_emissions, yellow_trips_2024, green_trips_2024
    """
    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # vehicle_emissions table
        con.execute(f"""
	        DROP TABLE IF EXISTS vehicle_emissions;
	    """)
        print("Dropped table if exists: vehicle_emissions")
        logger.info("Dropped table if exists: vehicle_emissions")

        con.execute(f"""
            CREATE TABLE vehicle_emissions AS
            SELECT * FROM read_csv_auto('data/vehicle_emissions.csv', header=True);
        """)
        print("Created table vehicle_emissions from CSV")
        logger.info("Created table vehicle_emissions from CSV")

        # row count
        count = con.execute("""
            SELECT COUNT(*) FROM vehicle_emissions;
        """)
        rows = count.fetchone()[0]
        print(f"Number of rows in vehicle_emissions: {rows:,}")
        logger.info(f"Number of rows in vehicle_emissions: {rows:,}")

        # Build 2024 monthly parquet urls
        base = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        yellow_urls = [f"{base}/yellow_tripdata_2024-{m:02d}.parquet" for m in range(1, 13)]
        green_urls  = [f"{base}/green_tripdata_2024-{m:02d}.parquet"  for m in range(1, 13)]

        # YELLOW 2024 table
        con.execute(f"""
            DROP TABLE IF EXISTS yellow_trips_2024;
            
            CREATE TABLE yellow_trips_2024 AS
            SELECT * FROM read_parquet('{yellow_urls[0]}');
        """)
        print("Created yellow_trips_2024 from January file")
        logger.info("Created yellow_trips_2024 from January file")

        # insert remaining months
        for u in yellow_urls[1:]:
            con.execute(f"INSERT INTO yellow_trips_2024 SELECT * FROM read_parquet('{u}');")
        print("Inserted remaining Yellow 2024 months")
        logger.info("Inserted remaining Yellow 2024 months")

        y_count = con.execute("""
            SELECT COUNT(*) FROM yellow_trips_2024;
        """).fetchone()[0]
        print(f"Number of rows in yellow_trips_2024: {y_count:,}")
        logger.info(f"Number of rows in yellow_trips_2024: {y_count:,}")

        # GREEN 2024 table
        con.execute(f"""
            DROP TABLE IF EXISTS green_trips_2024;
            
            CREATE TABLE green_trips_2024 AS
            SELECT * FROM read_parquet('{green_urls[0]}');
        """)
        print("Created green_trips_2024 from January file")
        logger.info("Created green_trips_2024 from January file")

        # insert remaining months
        for u in green_urls[1:]:
            con.execute(f"INSERT INTO green_trips_2024 SELECT * FROM read_parquet('{u}');")
        print("Inserted remaining Green 2024 months")
        logger.info("Inserted remaining Green 2024 months")

        g_count = con.execute(f"""
            SELECT COUNT(*) FROM green_trips_2024;
        """).fetchone()[0]
        print(f"Number of rows in green_trips_2024: {g_count:,}")
        logger.info(f"Number of rows in green_trips_2024: {g_count:,}")

        # descriptive stats for yellow
        stats = con.execute(f"""
            SELECT
                MIN(tpep_pickup_datetime) AS first_pickup,
                MAX(tpep_dropoff_datetime) AS last_dropoff,
                AVG(trip_distance) AS avg_distance,
                MIN(trip_distance) AS min_distance,
                MAX(trip_distance) AS max_distance,
            FROM yellow_trips_2024;
        """).fetchone()
        print(f"\nYellow Trips 2024 Descriptive Stats - "
              f"first pickup: {stats[0]}, last dropoff: {stats[1]}, "
              f"average distance: {stats[2]:.2f}, min distance: {stats[3]}, max distance: {stats[4]}")
        logger.info(f"Yellow Trips 2024 Descriptive Stats: {stats}")

        # descriptive stats for green
        stats = con.execute(f"""
            SELECT 
                MIN(lpep_pickup_datetime) AS first_pickup,
                MAX(lpep_dropoff_datetime) AS last_dropoff,
                AVG(trip_distance) AS avg_distance,
                MIN(trip_distance) AS min_distance,
                MAX(trip_distance) AS max_distance
            FROM green_trips_2024;
        """).fetchone()
        print(f"Green trips — first pickup: {stats[0]}, last dropoff: {stats[1]}, "
            f"avg_distance: {stats[2]:.2f}, min_distance: {stats[3]}, max_distance: {stats[4]}")
        logger.info(f"Green trips stats: {stats}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()
    print("Data loading complete.")
    logger.info("Data loading complete.")