from neo4j import GraphDatabase

uri = "neo4j+s://7245d5b4.databases.neo4j.io"
username = "7245d5b4"
password = "HHH-M3WJbojBqY0jNsswMFj1FjODPiN7qZyZKkWccIg"

driver = GraphDatabase.driver(uri, auth=(username, password))

def run_query_1():
    print("=" * 150)
    print("QUERY 1: Top 5 Popular Rooms by Major and Year")
    print("=" * 150)
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
                   rooms[2].visits as visits_3,
                   rooms[3].room as top_room_4,
                   rooms[3].visits as visits_4,
                   rooms[4].room as top_room_5,
                   rooms[4].visits as visits_5
            ORDER BY major, year ASC
        """)

        print(f"{'Major':<30} {'Year':<6} {'1st Room':<12} {'Visits':<8} {'2nd Room':<12} {'Visits':<8} {'3rd Room':<12} {'Visits':<8} {'4th Room':<12} {'Visits':<8} {'5th Room':<12} {'Visits':<8}")
        print("-" * 150)
        for record in result:
            print(f"{record['major']:<30} {record['year']:<6} "
                  f"{record['top_room_1']:<12} {record['visits_1']:<8} "
                  f"{record['top_room_2']:<12} {record['visits_2']:<8} "
                  f"{record['top_room_3']:<12} {record['visits_3']:<8} "
                  f"{record['top_room_4']:<12} {record['visits_4']:<8} "
                  f"{record['top_room_5']:<12} {record['visits_5']:<8}")

def run_query_2():
    print("\n" + "=" * 150)
    print("QUERY 2: Student Library Visits by Hour (Major and Year Breakdown)")
    print("=" * 150)
    with driver.session() as session:
        result = session.run("""
            MATCH (e:Event)-[:AT_LIBRARY]->(l:Library)
            WHERE e.timestamp IS NOT NULL
              AND e.event_type = 'ENTRY'
            OPTIONAL MATCH (s:Student {student_id: e.student_id})
            WHERE s.major IS NOT NULL
              AND s.year_of_study IS NOT NULL
            WITH s.major as major,
                 s.year_of_study as year,
                 e.timestamp.hour as hour,
                 COUNT(*) as total_visits,
                 COUNT(DISTINCT e.student_id) as unique_students
            RETURN major,
                   year,
                   CASE
                     WHEN hour = 0 THEN '12:00 AM'
                     WHEN hour < 12 THEN toString(hour) + ':00 AM'
                     WHEN hour = 12 THEN '12:00 PM'
                     ELSE toString(hour - 12) + ':00 PM'
                   END as time,
                   CASE
                     WHEN hour >= 6 AND hour <= 11 THEN 'Morning (6am-11am)'
                     WHEN hour >= 12 AND hour <= 16 THEN 'Afternoon (12pm-4pm)'
                     WHEN hour >= 17 AND hour <= 20 THEN 'Evening (5pm-8pm)'
                     ELSE 'Night (9pm-5am)'
                   END as time_period,
                   total_visits,
                   unique_students
            ORDER BY major, year, hour
        """)

        print(f"{'Major':<30} {'Year':<6} {'Time':<12} {'Period':<30} {'Total Visits':<12} {'Unique Students':<16}")
        print("-" * 150)
        for record in result:
            print(f"{record['major']:<30} {record['year']:<6} "
                  f"{record['time']:<12} {record['time_period']:<30} "
                  f"{record['total_visits']:<12} {record['unique_students']:<16}")

def run_summary():
    print("\n" + "=" * 70)
    print("SUMMARY STATISTICS")
    print("=" * 70)
    with driver.session() as session:
        result = session.run("""
            MATCH (e:Event)-[:AT_LIBRARY]->(l:Library)
            WHERE e.event_type = 'ENTRY'
            RETURN COUNT(*) as total_entries,
                   COUNT(DISTINCT e.student_id) as unique_students,
                   COUNT(DISTINCT date(e.timestamp)) as unique_days
        """)
        for record in result:
            print(f"Total Library Entries: {record['total_entries']}")
            print(f"Unique Students: {record['unique_students']}")
            print(f"Unique Days: {record['unique_days']}")
            print(f"Average Visits per Student: {record['total_entries'] / record['unique_students']:.1f}")
            print(f"Average Daily Visits: {record['total_entries'] / record['unique_days']:.1f}")

def run_peak_hours():
    print("\n" + "=" * 70)
    print("PEAK HOURS ANALYSIS (All Majors Combined)")
    print("=" * 70)
    with driver.session() as session:
        result = session.run("""
            MATCH (e:Event)-[:AT_LIBRARY]->(l:Library)
            WHERE e.event_type = 'ENTRY'
            RETURN e.timestamp.hour as hour,
                   CASE
                     WHEN e.timestamp.hour = 0 THEN '12:00 AM'
                     WHEN e.timestamp.hour < 12 THEN toString(e.timestamp.hour) + ':00 AM'
                     WHEN e.timestamp.hour = 12 THEN '12:00 PM'
                     ELSE toString(e.timestamp.hour - 12) + ':00 PM'
                   END as time,
                   COUNT(*) as visits
            ORDER BY visits DESC
            LIMIT 5
        """)
        print("Top 5 Peak Hours:")
        for record in result:
            print(f"  {record['time']} ({record['hour']}:00) - {record['visits']} visits")

def run_most_popular_rooms():
    print("\n" + "=" * 70)
    print("MOST POPULAR ROOMS (Overall)")
    print("=" * 70)
    with driver.session() as session:
        result = session.run("""
            MATCH (e:Event)-[:IN_ROOM]->(r:Room)
            WHERE e.event_type = 'ENTRY'
              AND r.location <> 'MAIN_HALL'
            RETURN r.location as room,
                   COUNT(*) as visits
            ORDER BY visits DESC
            LIMIT 5
        """)
        print("Top 5 Most Popular Rooms:")
        for record in result:
            print(f"  Room {record['room']}: {record['visits']} visits")

# Run all queries
run_query_1()
run_query_2()
run_summary()
run_peak_hours()
run_most_popular_rooms()

driver.close()