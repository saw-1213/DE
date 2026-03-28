# Author: Tee Min Jie

import json
from neo4j import GraphDatabase
from utils.config_manager import ConfigManager

def run_query_1(driver):
    print("=" * 105)
    print("QUERY 1: Top 3 Popular Rooms by Major and Year")
    print("=" * 105)
    with driver.session() as session:
        result = session.run("""
            MATCH (s:Student)-[:ENTERED]->(r:Room)
            WHERE s.major IS NOT NULL
              AND s.year_of_study IS NOT NULL
              AND r.location <> 'MAIN_HALL'
            WITH s.major as major,
                 s.year_of_study as year,
                 r.location as room,
                 COUNT(*) as visits
            ORDER BY major, year, visits DESC
            WITH major, year, collect({room: room, visits: visits}) as rooms
            RETURN major,
                   year,
                   rooms[0].room as top_room_1,
                   rooms[0].visits as visits_1,
                   rooms[1].room as top_room_2,
                   rooms[1].visits as visits_2,
                   rooms[2].room as top_room_3,
                   rooms[2].visits as visits_3
            ORDER BY major, year ASC
        """)

        print(f"{'Major':<30} {'Year':<6} {'1st Room':<12} {'Visits':<8} {'2nd Room':<12} {'Visits':<8} {'3rd Room':<12} {'Visits':<8}")
        print("-" * 105)

        # Check if there are results
        records = list(result)
        if not records:
            print("No data found for Query 1")
            return

        for record in records:
            print(f"{record['major']:<30} {record['year']:<6} "
                  f"{record['top_room_1']:<12} {record['visits_1']:<8} "
                  f"{record['top_room_2']:<12} {record['visits_2']:<8} "
                  f"{record['top_room_3']:<12} {record['visits_3']:<8}")

def run_query_2(driver):
    print("\n" + "=" * 80)
    print("QUERY 2: Daily Library Visits by Time Period")
    print("=" * 80)
    with driver.session() as session:
        # First check if there are entries
        check_result = session.run("""
            MATCH (e:Event)
            WHERE e.event_type = 'ENTRY'
              AND e.gate_type = 'MAIN_GATE'
            RETURN COUNT(*) as total_entries
        """)
        total_check = check_result.single()['total_entries']

        if total_check == 0:
            print("No entry data found for Query 2")
            return

        result = session.run("""
MATCH (e:Event)
WHERE e.event_type = 'ENTRY'
  AND e.gate_type = 'MAIN_GATE'

WITH e.date as visit_date,
     CASE
        WHEN e.time.hour >= 6 AND e.time.hour <= 11 THEN 'Morning'
        WHEN e.time.hour >= 12 AND e.time.hour <= 16 THEN 'Afternoon'
        WHEN e.time.hour >= 17 AND e.time.hour <= 20 THEN 'Evening'
        ELSE 'Night'
     END as time_period,
     e.student_id as student_id

RETURN toString(visit_date) as visit_date,
       time_period,
       COUNT(*) as total_visits,
       COUNT(DISTINCT student_id) as unique_students
ORDER BY visit_date DESC,
         CASE time_period
            WHEN 'Morning' THEN 1
            WHEN 'Afternoon' THEN 2
            WHEN 'Evening' THEN 3
            WHEN 'Night' THEN 4
         END
        """)

        print(f"{'Date':<15} {'Time Period':<20} {'Visits':<15} {'Unique Students':<18}")
        print("-" * 80)

        record_count = 0
        for record in result:
            record_count += 1
            print(f"{record['visit_date']:<15} {record['time_period']:<20} "
                  f"{record['total_visits']:<15} {record['unique_students']:<18}")

        if record_count == 0:
            print("No data found for Query 2")

def run_summary(driver):
    print("\n" + "=" * 70)
    print("SUMMARY STATISTICS")
    print("=" * 70)
    with driver.session() as session:
        # Check if there are entries first
        check_result = session.run("""
            MATCH (e:Event)
            WHERE e.event_type = 'ENTRY'
              AND e.gate_type = 'MAIN_GATE'
            RETURN COUNT(*) as total_entries
        """)
        total_check = check_result.single()['total_entries']

        if total_check == 0:
            print("No entry data found for summary statistics")
            return

        result = session.run("""
            MATCH (e:Event)
            WHERE e.event_type = 'ENTRY'
              AND e.gate_type = 'MAIN_GATE'
            RETURN COUNT(*) as total_entries,
                   COUNT(DISTINCT e.student_id) as unique_students,
                   COUNT(DISTINCT e.date) as unique_days
        """)

        for record in result:
            total = record['total_entries']
            students = record['unique_students']
            days = record['unique_days']

            # Avoid division by zero
            if students == 0 or days == 0:
                print("Insufficient data for summary statistics")
                return

            visits_per_student = total / students
            visits_per_day = total / days
            visits_per_student_per_day = visits_per_student / days

            print(f"Total Library Entries: {total}")
            print(f"Unique Students: {students}")
            print(f"Unique Days: {days}")
            print(f"\n📊 Averages:")
            print(f"  • Average Visits per Student (total over {days} days): {visits_per_student:.1f}")
            print(f"  • Average Daily Visits: {visits_per_day:.1f}")
            print(f"  • Average Visits per Student per Day: {visits_per_student_per_day:.2f} visits/day")
            print(f"\nSummary:")
            print(f"  Each student visited the library about {visits_per_student:.0f} times total")
            print(f"  That's roughly {visits_per_student_per_day:.1f} time(s) per day")

def run_peak_hours(driver):
    print("\n" + "=" * 70)
    print("PEAK HOURS ANALYSIS")
    print("=" * 70)
    with driver.session() as session:
        # First check if there are entries
        check_result = session.run("""
            MATCH (e:Event)
            WHERE e.event_type = 'ENTRY'
              AND e.gate_type = 'MAIN_GATE'
            RETURN COUNT(*) as total_entries
        """)
        total_check = check_result.single()['total_entries']

        if total_check == 0:
            print("No entry data found for peak hours analysis")
            return

        result = session.run("""
            MATCH (e:Event)
            WHERE e.event_type = 'ENTRY'
              AND e.gate_type = 'MAIN_GATE'
            WITH e.time.hour as local_hour
            RETURN local_hour,
                   CASE
                     WHEN local_hour = 0 THEN '12:00 AM'
                     WHEN local_hour < 12 THEN toString(local_hour) + ':00 AM'
                     WHEN local_hour = 12 THEN '12:00 PM'
                     ELSE toString(local_hour - 12) + ':00 PM'
                   END as time,
                   COUNT(*) as visits
            ORDER BY visits DESC
            LIMIT 5
        """)

        records = list(result)
        if not records:
            print("No data found for peak hours analysis")
            return

        print("Top 5 Peak Hours:")
        for record in records:
            print(f"  {record['time']} ({record['local_hour']}:00) - {record['visits']} visits")

def run_most_popular_rooms(driver):
    print("\n" + "=" * 70)
    print("MOST POPULAR ROOMS (Overall)")
    print("=" * 70)
    with driver.session() as session:
        # First check if there are room entries
        check_result = session.run("""
            MATCH (e:Event)-[:IN_ROOM]->(r:Room)
            WHERE e.event_type = 'ENTRY'
              AND r.location <> 'MAIN_HALL'
            RETURN COUNT(*) as total_visits
        """)
        total_check = check_result.single()['total_visits']

        if total_check == 0:
            print("No room visit data found")
            return

        result = session.run("""
            MATCH (e:Event)-[:IN_ROOM]->(r:Room)
            WHERE e.event_type = 'ENTRY'
              AND r.location <> 'MAIN_HALL'
            RETURN r.location as room,
                   COUNT(*) as visits
            ORDER BY visits DESC
            LIMIT 5
        """)

        records = list(result)
        if not records:
            print("No room visit data found")
            return

        print("Top 5 Most Popular Rooms:")
        for record in records:
            print(f"  Room {record['room']}: {record['visits']} visits")

def main():
    print("=" * 70)
    print("LIBRARY ANALYTICS - NEO4J QUERIES")
    print("=" * 70)

    try:
        # Load configuration
        config_mgr = ConfigManager('utils/config.json')
        neo4j_config = config_mgr.get_neo4j_config()

        # Connect to Neo4j
        driver = GraphDatabase.driver(
            neo4j_config['uri'],
            auth=(neo4j_config['username'], neo4j_config['password'])
        )

        driver.verify_connectivity()
        print("Successfully connected to Neo4j\n")

        # Check if data exists
        with driver.session() as session:
            result = session.run("MATCH (e:Event) RETURN count(e) as event_count")
            event_count = result.single()['event_count']

            if event_count == 0:
                print("WARNING: No data found in Neo4j database!")
                print("Please run batch_processing_all.py first to load data from HDFS into Neo4j")
                return

            print(f"Found {event_count} events in Neo4j. Running queries...\n")

        # Run all queries
        run_query_1(driver)
        run_query_2(driver)
        run_summary(driver)
        run_peak_hours(driver)
        run_most_popular_rooms(driver)

    except FileNotFoundError:
        print("\nError: config.json not found in current directory")
        print("Make sure config.json exists with Neo4j connection details")
    except KeyError as e:
        print(f"\nError: Missing key in config.json: {e}")
        print("Make sure config.json has: neo4j_uri, neo4j_username, neo4j_password")
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        if 'driver' in locals():
            driver.close()
            print("\n" + "=" * 70)
            print("ANALYTICS COMPLETE")
            print("=" * 70)

if __name__ == "__main__":
    main()