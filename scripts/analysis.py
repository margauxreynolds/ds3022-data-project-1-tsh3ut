import duckdb
import logging
import matplotlib.pyplot as plt

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/analysis.log'
)
logger = logging.getLogger(__name__)

def analyze_parquet_files():
    """
    Prints 6 analysis results for YELLOW and GREEN and saves a PNG plot: 
    output/co2_by_month.png
    """
    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        yellow = "yellow_trips_2024_transformed"
        green  = "green_trips_2024_transformed"

        # What was the single largest carbon producing trip of the year for YELLOW and GREEN trips? (One result for each type)
        y_max = con.execute(f"SELECT MAX(trip_co2_kgs) FROM {yellow};").fetchone()[0]
        g_max = con.execute(f"SELECT MAX(trip_co2_kgs) FROM {green};").fetchone()[0]
        print(f"1) Largest CO2 trip (kg) - YELLOW: {y_max}, GREEN: {g_max}")
        logger.info(f"1) Largest CO2 trip (kg) - YELLOW: {y_max}, GREEN: {g_max}")

        # Across the entire year, what on average are the most carbon heavy and carbon light hours of the day for YELLOW and for GREEN trips? (1-24)
        y_hour_heavy = con.execute(f"""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_kg
            FROM {yellow} GROUP BY hour_of_day
            ORDER BY avg_kg DESC LIMIT 1;
        """).fetchone()
        y_hour_light = con.execute(f"""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_kg
            FROM {yellow} GROUP BY hour_of_day
            ORDER BY avg_kg ASC LIMIT 1;
        """).fetchone()

        g_hour_heavy = con.execute(f"""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_kg
            FROM {green} GROUP BY hour_of_day
            ORDER BY avg_kg DESC LIMIT 1;
        """).fetchone()
        g_hour_light = con.execute(f"""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_kg
            FROM {green} GROUP BY hour_of_day
            ORDER BY avg_kg ASC LIMIT 1;
        """).fetchone()

        print(f"2) Hour (avg CO2 kg per trip) — "
              f"YELLOW HEAVY: {y_hour_heavy[0]} ({y_hour_heavy[1]:.4f}), "
              f"YELLOW LIGHT: {y_hour_light[0]} ({y_hour_light[1]:.4f}); "
              f"GREEN HEAVY: {g_hour_heavy[0]} ({g_hour_heavy[1]:.4f}), "
              f"GREEN LIGHT: {g_hour_light[0]} ({g_hour_light[1]:.4f})")
        logger.info(f"2) Hour (avg CO2 kg per trip) — "
              f"YELLOW HEAVY: {y_hour_heavy[0]} ({y_hour_heavy[1]:.4f}), "
              f"YELLOW LIGHT: {y_hour_light[0]} ({y_hour_light[1]:.4f}); "
              f"GREEN HEAVY: {g_hour_heavy[0]} ({g_hour_heavy[1]:.4f}), "
              f"GREEN LIGHT: {g_hour_light[0]} ({g_hour_light[1]:.4f})")

        # Across the entire year, what on average are the most carbon heavy and carbon light days of the week for YELLOW and for GREEN trips? (Sun-Sat)
        y_dow_heavy = con.execute(f"""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_kg
            FROM {yellow} GROUP BY day_of_week
            ORDER BY avg_kg DESC LIMIT 1;
        """).fetchone()
        y_dow_light = con.execute(f"""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_kg
            FROM {yellow} GROUP BY day_of_week
            ORDER BY avg_kg ASC LIMIT 1;
        """).fetchone()

        g_dow_heavy = con.execute(f"""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_kg
            FROM {green} GROUP BY day_of_week
            ORDER BY avg_kg DESC LIMIT 1;
        """).fetchone()
        g_dow_light = con.execute(f"""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_kg
            FROM {green} GROUP BY day_of_week
            ORDER BY avg_kg ASC LIMIT 1;
        """).fetchone()

        dow = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]
        print(f"3) Day of week (avg CO2 kg per trip) — "
              f"YELLOW HEAVY: {dow[int(y_dow_heavy[0])]} ({y_dow_heavy[1]:.4f}), "
              f"YELLOW LIGHT: {dow[int(y_dow_light[0])]} ({y_dow_light[1]:.4f}); "
              f"GREEN HEAVY: {dow[int(g_dow_heavy[0])]} ({g_dow_heavy[1]:.4f}), "
              f"GREEN LIGHT: {dow[int(g_dow_light[0])]} ({g_dow_light[1]:.4f})")
        logger.info(f"3) Day of week (avg CO2 kg per trip) — "
              f"YELLOW HEAVY: {dow[int(y_dow_heavy[0])]} ({y_dow_heavy[1]:.4f}), "
              f"YELLOW LIGHT: {dow[int(y_dow_light[0])]} ({y_dow_light[1]:.4f}); "
              f"GREEN HEAVY: {dow[int(g_dow_heavy[0])]} ({g_dow_heavy[1]:.4f}), "
              f"GREEN LIGHT: {dow[int(g_dow_light[0])]} ({g_dow_light[1]:.4f})")

        # Across the entire year, what on average are the most carbon heavy and carbon light weeks of the year for YELLOW and for GREEN trips? (1-52)
        y_week_heavy = con.execute(f"""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM {yellow} GROUP BY week_of_year
            ORDER BY avg_kg DESC LIMIT 1;
        """).fetchone()
        y_week_light = con.execute(f"""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM {yellow} GROUP BY week_of_year
            ORDER BY avg_kg ASC LIMIT 1;
        """).fetchone()

        g_week_heavy = con.execute(f"""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM {green} GROUP BY week_of_year
            ORDER BY avg_kg DESC LIMIT 1;
        """).fetchone()
        g_week_light = con.execute(f"""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM {green} GROUP BY week_of_year
            ORDER BY avg_kg ASC LIMIT 1;
        """).fetchone()

        print(f"4) Week of year (avg CO2 kg per trip) — "
              f"YELLOW HEAVY: {int(y_week_heavy[0])} ({y_week_heavy[1]:.4f}), "
              f"YELLOW LIGHT: {int(y_week_light[0])} ({y_week_light[1]:.4f}); "
              f"GREEN HEAVY: {int(g_week_heavy[0])} ({g_week_heavy[1]:.4f}), "
              f"GREEN LIGHT: {int(g_week_light[0])} ({g_week_light[1]:.4f})")
        logger.info(f"4) Week of year (avg CO2 kg per trip) — "
              f"YELLOW HEAVY: {int(y_week_heavy[0])} ({y_week_heavy[1]:.4f}), "
              f"YELLOW LIGHT: {int(y_week_light[0])} ({y_week_light[1]:.4f}); "
              f"GREEN HEAVY: {int(g_week_heavy[0])} ({g_week_heavy[1]:.4f}), "
              f"GREEN LIGHT: {int(g_week_light[0])} ({g_week_light[1]:.4f})")
        
        # Across the entire year, what on average are the most carbon heavy and carbon light months of the year for YELLOW and for GREEN trips? (Jan-Dec)
        y_month_heavy = con.execute(f"""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM {yellow} GROUP BY month_of_year
            ORDER BY avg_kg DESC LIMIT 1;
        """).fetchone()
        y_month_light = con.execute(f"""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM {yellow} GROUP BY month_of_year
            ORDER BY avg_kg ASC LIMIT 1;
        """).fetchone()

        g_month_heavy = con.execute(f"""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM {green} GROUP BY month_of_year
            ORDER BY avg_kg DESC LIMIT 1;
        """).fetchone()
        g_month_light = con.execute(f"""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM {green} GROUP BY month_of_year
            ORDER BY avg_kg ASC LIMIT 1;
        """).fetchone()

        print(f"5) Month (avg CO2 kg per trip) — "
              f"YELLOW HEAVY: {int(y_month_heavy[0])} ({y_month_heavy[1]:.4f}), "
              f"YELLOW LIGHT: {int(y_month_light[0])} ({y_month_light[1]:.4f}); "
              f"GREEN HEAVY: {int(g_month_heavy[0])} ({g_month_heavy[1]:.4f}), "
              f"GREEN LIGHT: {int(g_month_light[0])} ({g_month_light[1]:.4f})")
        logger.info("Month results computed")

        # Generate a time-series plot or histogram with MONTH along the X-axis and CO2 totals along the Y-axis. Render two lines/bars/plots of data, one each for YELLOW and GREEN taxi trip CO2 totals
        y_month_totals = con.execute(f"""
            SELECT month_of_year AS m, SUM(trip_co2_kgs) AS total_kg
            FROM {yellow} GROUP BY m ORDER BY m;
        """).fetchall()
        g_month_totals = con.execute(f"""
            SELECT month_of_year AS m, SUM(trip_co2_kgs) AS total_kg
            FROM {green} GROUP BY m ORDER BY m;
        """).fetchall()

        y_m = [int(r[0]) for r in y_month_totals]; y_tot = [float(r[1]) for r in y_month_totals]
        g_m = [int(r[0]) for r in g_month_totals]; g_tot = [float(r[1]) for r in g_month_totals]

        plt.figure(figsize=(9,5))
        plt.plot(y_m, y_tot, label="Yellow CO2 total (kg)")
        plt.plot(g_m, g_tot, label="Green CO2 total (kg)")
        plt.title("Total CO2 by Month (2024)")
        plt.xlabel("Month (1–12)")
        plt.ylabel("Total CO2 (kg)")
        plt.legend()
        plt.tight_layout()
        out = "output/co2_by_month.png"
        plt.savefig(out)
        plt.close()

        print(f"6) Plot saved to {out}")
        logger.info(f"Plot saved to {out}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    analyze_parquet_files()
    print("Data analysis complete.")
    logger.info("Data analysis complete.")