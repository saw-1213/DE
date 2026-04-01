# Author: Yap Wan Shuen

from pymongo import MongoClient
import json
from collections import defaultdict

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

        student_map = {}
        for s in self.students.find():
            student_map[s["student_id"]] = s.get("major", "Unknown")

        student_visits = defaultdict(int)
        for e in self.events.find():
            student_visits[e["student_id"]] += 1

        major_data = defaultdict(list)
        for sid, visits in student_visits.items():
            major = student_map.get(sid, "Unknown")
            major_data[major].append(visits)

        major_avg = {}
        for major, visits_list in major_data.items():
            major_avg[major] = sum(visits_list) / len(visits_list)

        if not major_avg:
            print("No data found.")
            return

        overall_avg = sum(major_avg.values()) / len(major_avg)

        below_avg = {m: v for m, v in major_avg.items() if v < overall_avg}

        if not below_avg:
            print("No majors below overall average.")
            return

        print(f"\nOverall Avg Visits: {round(overall_avg, 2)}\n")
        print(f"{'Major':<30} {'Avg Visits':>10}")
        print("-" * 42)

        for major, avg in sorted(below_avg.items(), key=lambda x: x[1]):
            print(f"{major:<30} {round(avg, 2):>10}")

    def query_2(self):
        print("\nQuery 2: Top 3 Students With Most Entries in a Day")

        student_map = {}
        for s in self.students.find():
            student_map[s["student_id"]] = s.get("major", "N/A")

        entry_count = defaultdict(int)
        for e in self.events.find():
            key = (e["student_id"], e["date"])
            entry_count[key] += 1

        top_3 = sorted(entry_count.items(), key=lambda x: x[1], reverse=True)[:3]

        if not top_3:
            print("No data found.")
            return

        print(f"\n{'Student ID':<12} {'Major':<25} {'Date':<12} {'Entries':>7}")
        print("-" * 60)

        for (student_id, date), entries in top_3:
            major = student_map.get(student_id, "N/A")
            print(f"{student_id:<12} {major:<25} {date:<12} {entries:>7}")

    def run(self):
        self.query_1()
        self.query_2()


if __name__ == "__main__":
    mq = MongoQueries("utils/config.json")
    mq.run()
