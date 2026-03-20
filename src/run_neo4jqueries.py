from neo4j import GraphDatabase

uri = "neo4j+s://7245d5b4.databases.neo4j.io"
username = "7245d5b4"
password = "HHH-M3WJbojBqY0jNsswMFj1FjODPiN7qZyZKkWccIg"

driver = GraphDatabase.driver(uri, auth=(username, password))

def run_query_1():
    print("=" * 90)
    print("QUERY 1: Top 5 Popular Rooms by Major and Year")
    print("=" * 90)
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
        
        print(f"{'Major':<30} {'Year':<6} {'1st Room':<10} {'Visits':<6} {'2nd Room':<10} {'Visits':<6} {'3rd Room':<10} {'Visits':<6}")
        print("-" * 95)
        for record in result:
            print(f"{record['major']:<30} {record['year']:<6} "
                  f"{record['top_room_1']:<10} {record['visits_1']:<6} "
                  f"{record['top_room_2']:<10} {record['visits_2']:<6} "
                  f"{record['top_room_3']:<10} {record['visits_3']:<6}")

def run_query_2():
    print("\n" + "=" * 100)
    print("QUERY 2: Student Library Visits by Hour (Major and Year Breakdown)")
    print("=" * 100)
    with driver.session() as session:
        result = session.run("""
            MATCH (e:Event)-[:AT_LIBRARY]->(l:Library)
            WHERE e.timestamp IS NOT NULL
              AND e.event_type = 'ENTRY'
            OPTIONAL MATCH (s:Student {student_id: e.student_id})
            WHERE s.major IS NOT NULL
              AND s.year_of_study IS NOT NULL
            RETURN s.major as major,
                   s.year_of_study as year,
                   CASE 
                     WHEN e.timestamp.hour = 0 THEN '12:00 AM'
                     WHEN e.timestamp.hour < 12 THEN toString(e.timestamp.hour) + ':00 AM'
                     WHEN e.timestamp.hour = 12 THEN '12:00 PM'
                     ELSE toString(e.timestamp.hour - 12) + ':00 PM'
                   END as time,
                   CASE
                     WHEN e.timestamp.hour >= 6 AND e.timestamp.hour <= 11 THEN 'Morning (6am-11am)'
                     WHEN e.timestamp.hour >= 12 AND e.timestamp.hour <= 16 THEN 'Afternoon (12pm-4pm)'
                     WHEN e.timestamp.hour >= 17 AND e.timestamp.hour <= 20 THEN 'Evening (5pm-8pm)'
                     ELSE 'Night (9pm-5am)'
                   END as time_period,
                   COUNT(*) as total_visits,
                   COUNT(DISTINCT e.student_id) as unique_students
            ORDER BY major, year
        """)
        
        print(f"{'Major':<30} {'Year':<6} {'Time':<12} {'Period':<25} {'Total Visits':<12} {'Unique Students':<16}")
        print("-" * 105)
        for record in result:
            print(f"{record['major']:<30} {record['year']:<6} "
                  f"{record['time']:<12} {record['time_period']:<25} "
                  f"{record['total_visits']:<12} {record['unique_students']:<16}")

def run_summary():
    print("\n" + "=" * 60)
    print("SUMMARY STATISTICS")
    print("=" * 60)
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

# Run all queries
run_query_1()
run_query_2()
run_summary()

driver.close()