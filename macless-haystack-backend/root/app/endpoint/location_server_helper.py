import datetime
import hashlib
import os
import sqlite3

import folium
import pandas as pd
from folium.plugins import AntPath
from location_server_common import HISTORY_DAYS, db_path, logger, state_dir


def generate_filename(id_short, id_value):
    combined = id_short + id_value
    hash_object = hashlib.sha3_256(combined.encode("utf-8"))
    filename = hash_object.hexdigest() + ".html"
    return filename


def format_time(seconds):
    if not seconds or seconds != seconds:  # Check for NaN
        return "0s"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"


def generate_map_for_device(df, device_name, filename):
    try:
        # Process the data
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["lat"] = df["lat"].astype(float)
        df["lon"] = df["lon"].astype(float)
        df["datetime"] = df["timestamp"]
        df["isodatetime"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        df["time_diff"] = df["timestamp"].diff().dt.total_seconds()

        # Calculate statistics
        average_time_diff = df["time_diff"][1:].mean()
        time_diff_total = (
            df.iloc[-1]["timestamp"] - df.iloc[0]["timestamp"]
        ).total_seconds()
        formatted_total_time = format_time(time_diff_total)
        formatted_avg_time = format_time(average_time_diff)
        start_timestamp = df.iloc[0]["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
        end_timestamp = df.iloc[-1]["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
        ping_count = df.shape[0]

        # Create the map
        map_center = [df.iloc[0]["lat"], df.iloc[0]["lon"]]
        m = folium.Map(location=map_center, zoom_start=13)

        latlon_pairs = list(zip(df["lat"], df["lon"]))
        ant_path = AntPath(
            locations=latlon_pairs,
            dash_array=[10, 20],
            delay=1000,
            color="red",
            weight=5,
            pulse_color="black",
        )
        m.add_child(ant_path)

        # Location markers
        for index, row in df.iterrows():
            if index == df.index[0]:  # First marker
                folium.Marker(
                    [row["lat"], row["lon"]],
                    popup=f"Timestamp: {row['isodatetime']} Start Point",
                    tooltip=f"Start Point",
                    icon=folium.Icon(color="green"),
                ).add_to(m)
            elif index == df.index[-1]:  # Last marker
                folium.Marker(
                    [row["lat"], row["lon"]],
                    popup=f"Timestamp: {row['isodatetime']} End Point",
                    tooltip=f"End Point",
                    icon=folium.Icon(color="red"),
                ).add_to(m)
            else:  # Other markers
                folium.Marker(
                    [row["lat"], row["lon"]],
                    popup=f"Timestamp: {row['isodatetime']}",
                    tooltip=f"Point {index+1}",
                ).add_to(m)

        # Add title and info
        title_and_info_html = f"""
         <h3 align="center" style="font-size:20px; margin-top:10px;"><b>FindMy Location Mapper - {device_name}</b></h3>
         <div style="position: fixed; bottom: 50px; left: 50px; width: 300px; height: 160px; z-index:9999; font-size:14px; background-color: white; padding: 10px; border-radius: 10px; box-shadow: 0 0 5px rgba(0,0,0,0.5);">
         <b>Location Summary</b><br>
         Device: {device_name}<br>
         Start: {start_timestamp}<br>
         End: {end_timestamp}<br>
         Number of Location Pings: {ping_count}<br>
         Total Time: {formatted_total_time}<br>
         Average Time Between Pings: {formatted_avg_time}<br>
         </div>
         """
        m.get_root().html.add_child(folium.Element(title_and_info_html))

        # Save the map
        html_output_path = os.path.join(state_dir, "maps", filename)
        os.makedirs(os.path.dirname(html_output_path), exist_ok=True)
        m.save(html_output_path)
        logger.info(f"Map generated for device '{device_name}' at '{html_output_path}'")
    except Exception as e:
        logger.error(
            f"Error generating map for device {device_name}: {e}", exc_info=True
        )


def generate_index_html(device_map):
    index_html_path = os.path.join(state_dir, "index.html")
    try:
        with open(index_html_path, "w") as f:
            f.write(
                """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Device Maps</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@mdi/font@7.4.47/css/materialdesignicons.min.css" integrity="sha384-HphS8cQyN+eYiJ5PMbzShG6qZdRtvHPVLPkYb8JwMkmNgaIxrFVDhQe3jIbq3EZ2" crossorigin="anonymous">
  <style>
  body {
    font-family: sans-serif;
    margin: 0;
    padding: 20px;
  }

  h1 {
    margin-bottom: 16px;
  }

  .device-list {
    list-style: none;
    padding: 0;
    margin: 0;
  }

  .device-list-item {
    margin-bottom: 16px;
  }

  .device-list-item a {
    text-decoration: none;
    color: inherit;
    display: flex;
    align-items: center;
  }

  .device-list-item a:hover {
    color: #007bff;
  }

  .device-list-item .material-icons {
    margin-right: 16px;
    font-size: 24px;
    vertical-align: middle;
  }
  </style>
</head>
<body>
  <h1>Available Device Maps</h1>
  <ul class="device-list">
"""
            )
            for device_name, filename in device_map.items():
                f.write(
                    f'<li class="device-list-item"><a href="maps/{filename}"><span class="material-icons-sharp">location_on</span>{device_name}</a></li>\n'
                )
            f.write("</ul>\n</body>\n</html>")
            logger.info(f"'index.html' generated at '{index_html_path}'")
    except Exception as e:
        logger.error(f"Error generating 'index.html': {e}", exc_info=True)


def generate_html_map():
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path, check_same_thread=False)

        # Compute the timestamp for 'HISTORY_DAYS' days ago
        history_timestamp = int(
            (
                datetime.datetime.now() - datetime.timedelta(days=HISTORY_DAYS)
            ).timestamp()
        )

        # Get list of devices
        device_query = "SELECT DISTINCT id_short FROM reports"
        device_df = pd.read_sql_query(device_query, conn)
        devices = device_df["id_short"].tolist()

        if not devices:
            logger.info("No devices found in the database.")
            return

        device_map = (
            {}
        )  # Dictionary to store device names and corresponding map filenames

        for device in devices:
            # For this device, get the latest 500 records within HISTORY_DAYS
            data_query = """
                SELECT *
                FROM reports
                WHERE id_short = ?
                  AND timestamp >= ?
                ORDER BY timestamp DESC
                LIMIT 500
            """
            df = pd.read_sql_query(data_query, conn, params=(device, history_timestamp))

            if df.empty:
                logger.info(
                    f"No data for device {device} in the last {HISTORY_DAYS} days."
                )
                continue

            # Sort the dataframe by timestamp ascending
            df = df.sort_values("timestamp")

            # Get the 'id' value (assuming it's consistent per 'id_short')
            id_value = df.iloc[0]["id"]

            # Generate the filename using SHA3-256 hash
            filename = generate_filename(device, id_value)

            # Generate the map for this device
            generate_map_for_device(df, device, filename)

            # Add to device_map
            device_map[device] = filename

        conn.close()

        # Generate 'index.html' with links to each device map
        generate_index_html(device_map)

    except Exception as e:
        logger.error(f"Error generating HTML maps: {e}", exc_info=True)


def initialize_database():
    """
    Initializes the database by creating necessary tables, indexes, and triggers.
    """
    # Connect to the database
    conn = sqlite3.connect(db_path, check_same_thread=False)
    c = conn.cursor()

    # Create the reports table if it does not exist
    c.execute(
        """CREATE TABLE IF NOT EXISTS reports (
        id_short TEXT, timestamp INTEGER, datePublished INTEGER, payload TEXT, 
        id TEXT, statusCode INTEGER, lat REAL, lon REAL, conf INTEGER,
        PRIMARY KEY(id_short,timestamp));"""
    )

    # Create 'rowcount' table
    c.execute("CREATE TABLE IF NOT EXISTS rowcount(count INTEGER)")
    # Check if rowcount table is empty
    c.execute("SELECT COUNT(*) FROM rowcount")
    rowcount_exists = c.fetchone()[0]

    if not rowcount_exists:
        # Initialize count with current total number of rows in the reports table
        c.execute("SELECT COUNT(*) FROM reports")
        total_rows = c.fetchone()[0]
        c.execute("INSERT INTO rowcount(count) VALUES (?)", (total_rows,))

    # Create triggers
    c.execute(
        """
    CREATE TRIGGER IF NOT EXISTS reports_insert AFTER INSERT ON reports
    BEGIN
        UPDATE rowcount SET count = count + 1;
    END;
    """
    )

    c.execute(
        """
    CREATE TRIGGER IF NOT EXISTS reports_delete AFTER DELETE ON reports
    BEGIN
        UPDATE rowcount SET count = count - 1;
    END;
    """
    )

    # Create indexes
    c.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_reports_id_short ON reports(id_short)
    """
    )

    c.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_reports_id_short_timestamp ON reports(id_short, timestamp DESC)
    """
    )

    # Commit changes and close connection
    conn.commit()
    conn.close()
