# Author: Yap Wan Shuen

from pymongo import MongoClient
import json

class MongoQueries:
    def __init__(self, config_path):
        with open(config_path) as f:
            self.config = json.load(f)

        self.client = MongoClient(self.config["mongodb_uri"])
        self.db = self.client["library_db"]

        self.events = self.db["library_events"]
        self.students = self.db["students"]

    def query_1(self):
        print("\nQuery 1: Under-Engaged Majors (Below Overall Average Library Usage)")

        pipeline_avg = [
            {"$lookup": {
                "from": "students",
                "localField": "student_id",
                "foreignField": "student_id",
                "as": "student_info"
            }},
            {"$unwind": "$student_info"},
            {"$group": {
                "_id": {
                    "student_id": "$student_id",
                    "major": "$student_info.major"
                },
                "visits": {"$sum": 1}
            }},
            {"$group": {
                "_id": "$_id.major",
                "avgVisits": {"$avg": "$visits"}
            }},
            {"$sort": {"avgVisits": 1}}
        ]

        major_avgs = list(self.events.aggregate(pipeline_avg))

        if not major_avgs:
            print("No data found.")
            return

        overall_avg = sum(m['avgVisits'] for m in major_avgs) / len(major_avgs)

        below_avg = [m for m in major_avgs if m['avgVisits'] < overall_avg]

        if not below_avg:
            print("No majors below overall average.")
            return

        print(f"\nOverall Avg Visits: {round(overall_avg, 2)}\n")
        print(f"{'Major':<30} {'Avg Visits':>10}")
        print("-" * 42)
        for m in below_avg:
            print(f"{m['_id']:<30} {round(m['avgVisits'], 2):>10}")

    def query_2(self):
        print("\nQuery 2: Top 3 Students With Most Entries in a Day")

        pipeline = [
            {"$group": {
                "_id": {
                    "student_id": "$student_id",
                    "date": "$date"
                },
                "entries": {"$sum": 1}
            }},
            {"$sort": {"entries": -1}},
            {"$limit": 3},
            {"$lookup": {
                "from": "students",
                "localField": "_id.student_id",
                "foreignField": "student_id",
                "as": "student_info"
            }},
            {"$unwind": "$student_info"}
        ]

        results = list(self.events.aggregate(pipeline))
        if not results:
            print("No data found.")
            return

        print(f"\n{'Student ID':<12} {'Major':<25} {'Date':<12} {'Entries':>7}")
        print("-" * 60)
        for r in results:
            student_id = r['_id']['student_id']
            major = r['student_info'].get('major', 'N/A')
            date = r['_id']['date']
            entries = r['entries']
            print(f"{student_id:<12} {major:<25} {date:<12} {entries:>7}")

    def run(self):
        self.query_1()
        self.query_2()


if __name__ == "__main__":
    mq = MongoQueries("config.json")
    mq.run()


