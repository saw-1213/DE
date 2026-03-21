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
    print("QUERY 2: Daily Library Visits by Time Period with Top Most Visited Major and Year")
    print("=" * 150)
    with driver.session() as session:
        result = session.run("""
           MATCH (e:Event)-[:AT_LIBRARY]->(l:Library)
                WHERE e.timestamp IS NOT NULL
                AND e.event_type = 'ENTRY'
                AND e.timestamp >= datetime() - duration({days: 30})
                OPTIONAL MATCH (s:Student {student_id: e.student_id})
                WHERE s.major IS NOT NULL
                AND s.year_of_study IS NOT NULL
                WITH date(e.timestamp) as visit_date,
                CASE
                        WHEN (e.timestamp + duration({hours: 8})).hour >= 6 AND (e.timestamp + duration({hours: 8})).hour <= 11 THEN 'Morning'
                        WHEN (e.timestamp + duration({hours: 8})).hour >= 12 AND (e.timestamp + duration({hours: 8})).hour <= 16 THEN 'Afternoon'
                        WHEN (e.timestamp + duration({hours: 8})).hour >= 17 AND (e.timestamp + duration({hours: 8})).hour <= 20 THEN 'Evening'
                ELSE 'Night'
                END as time_period, e.student_id as student_id, s.major as major, s.year_of_study as year
                // First aggregate by date, time period, major, and year to get counts
                WITH visit_date, time_period, major, year,
                        COUNT(*) as year_major_visits,
                        COUNT(DISTINCT student_id) as unique_students_in_group
                // Now aggregate by date, time period, and major to collect year data
                WITH visit_date, time_period, major,
                        COLLECT({year: year, visits: year_major_visits}) as year_counts,
                        SUM(year_major_visits) as total_major_visits
                // For each date and time period, find the top major
                WITH visit_date, time_period,
                        COLLECT({major: major, total_visits: total_major_visits, year_counts: year_counts}) as majors
                WITH visit_date, time_period,
                        REDUCE(best = HEAD(majors), m in TAIL(majors) |
                CASE WHEN m.total_visits > best.total_visits THEN m ELSE best END) as top_major
                WITH visit_date, time_period,
                        top_major.major as most_visited_major,
                        top_major.year_counts as year_counts
                // For the top major, find the most common year
                WITH visit_date, time_period, most_visited_major,
                REDUCE(best = HEAD(year_counts), y in TAIL(year_counts) |
                CASE WHEN y.visits > best.visits THEN y ELSE best END) as top_year
                // Now get the total visits and unique students for each date and time period
                OPTIONAL MATCH (e2:Event)-[:AT_LIBRARY]->(l:Library)
                WHERE date(e2.timestamp) = visit_date AND e2.event_type = 'ENTRY' AND e2.timestamp >= datetime() - duration({days: 30})
                 AND CASE
                WHEN (e2.timestamp + duration({hours: 8})).hour >= 6 AND (e2.timestamp + duration({hours: 8})).hour <= 11 THEN 'Morning'
                WHEN (e2.timestamp + duration({hours: 8})).hour >= 12 AND (e2.timestamp + duration({hours: 8})).hour <= 16 THEN 'Afternoon'
                WHEN (e2.timestamp + duration({hours: 8})).hour >= 17 AND (e2.timestamp + duration({hours: 8})).hour <= 20 THEN 'Evening'
                ELSE 'Night'
                END = time_period
                WITH visit_date, time_period,
                COUNT(DISTINCT e2.student_id) as unique_students,
                COUNT(*) as visits, most_visited_major, top_year.year as most_visited_major_study_year
                RETURN toString(visit_date) as visit_date, time_period, visits, unique_students, most_visited_major, most_visited_major_study_year
                ORDER BY visit_date DESC,
                CASE time_period
                WHEN 'Morning' THEN 1
                WHEN 'Afternoon' THEN 2
                WHEN 'Evening' THEN 3
                WHEN 'Night' THEN 4
                END
        """)

        print(f"{'Date':<15} {'Time Period':<20} {'Visits':<15} {'Unique Students':<18} {'Top Most Visited Major':<30} {'Top Most Visited Major Study Year':<25}")
        print("-" * 150)
        for record in result:
            print(f"{record['visit_date']:<15} {record['time_period']:<20} "
                  f"{record['visits']:<15} {record['unique_students']:<18} "
                  f"{record['most_visited_major']:<30} {record['most_visited_major_study_year']:<25}")

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