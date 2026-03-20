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
    print("QUERY 2: Student Library Visits by Hour (Major and Year)")
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
                 (e.timestamp + duration({hours: 8})).hour as local_hour,
                 e.student_id as student_id
            WITH major, year, local_hour, student_id
            RETURN major,
                   year,
                   local_hour,
                   CASE
                     WHEN local_hour = 0 THEN '12:00 AM'
                     WHEN local_hour < 12 THEN toString(local_hour) + ':00 AM'
                     WHEN local_hour = 12 THEN '12:00 PM'
                     ELSE toString(local_hour - 12) + ':00 PM'
                   END as time,
                   CASE
                     WHEN local_hour >= 6 AND local_hour <= 11 THEN 'Morning (6am-11am)'
                     WHEN local_hour >= 12 AND local_hour <= 16 THEN 'Afternoon (12pm-4pm)'
                     WHEN local_hour >= 17 AND local_hour <= 20 THEN 'Evening (5pm-8pm)'
                     ELSE 'Night (9pm-5am)'
                   END as time_period,
                   COUNT(*) as visits,
                   COUNT(DISTINCT student_id) as unique_students
            ORDER BY major, year, local_hour
        """)

        print(f"{'Major':<30} {'Year':<6} {'Hour':<6} {'Time':<12} {'Period':<35} {'Visits':<10} {'Unique Students':<16}")
        print("-" * 150)
        for record in result:
            print(f"{record['major']:<30} {record['year']:<6} {record['local_hour']:<6} "
                  f"{record['time']:<12} {record['time_period']:<35} "
                  f"{record['visits']:<10} {record['unique_students']:<16}")

def run_summary():
    print("\n" + "=" * 70)
    print("SUMMARY STATISTICS")
    print("=" * 70)
    with driver.session() as session:
        result = session.run("""
            MATCH (e:Event)-[:AT_LIBRARY]->(l:Library)
            WHERE e.event_type = 'ENTRY'
            WITH e, date(e.timestamp + duration({hours: 8})) as local_date
            RETURN COUNT(*) as total_entries,
                   COUNT(DISTINCT e.student_id) as unique_students,
                   COUNT(DISTINCT local_date) as unique_days
        """)
        for record in result:
            total = record['total_entries']
            students = record['unique_students']
            days = record['unique_days']
            visits_per_student = total / students
            visits_per_day = total / days
            visits_per_student_per_day = visits_per_student / days

            print(f"Total Library Entries: {total}")
            print(f"Unique Students: {students}")
            print(f"Unique Days: {days}")
            print(f"\nAverages:")
            print(f"  • Average Visits per Student (total over {days} days): {visits_per_student:.1f}")
            print(f"  • Average Daily Visits: {visits_per_day:.1f}")
            print(f"  • Average Visits per Student per Day: {visits_per_student_per_day:.2f} visits/day")
            print(f"\nSummary:")
            print(f"  Each student visited the library about {visits_per_student:.0f} times total")
            print(f"  That's roughly {visits_per_student_per_day:.1f} time(s) per day")

def run_peak_hours():
    print("\n" + "=" * 70)
    print("PEAK HOURS ANALYSIS")
    print("=" * 70)
    with driver.session() as session:
        result = session.run("""
            MATCH (e:Event)-[:AT_LIBRARY]->(l:Library)
            WHERE e.event_type = 'ENTRY'
            WITH (e.timestamp + duration({hours: 8})).hour as local_hour
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
        print("Top 5 Peak Hours (Local Time):")
        for record in result:
            print(f"  {record['time']} ({record['local_hour']}:00) - {record['visits']} visits")

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