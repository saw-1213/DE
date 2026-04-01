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
        print("\nQuery 1: Top 5 Peak Entry Hours")

        pipeline = [
            {"$match": {"event_type": "ENTRY"}},
            {
                "$project": {
                    "hour": {"$substr": ["$time", 0, 2]}
                }
            },
            {
                "$group": {
                    "_id": "$hour",
                    "totalEntries": {"$sum": 1}
                }
            },
            {"$sort": {"totalEntries": -1}}
        ]

        results = list(self.events.aggregate(pipeline))

        if not results:
            print("No data found.")
            return

        total_all = sum(r["totalEntries"] for r in results)
        top5 = results[:5]

        print(f"\n{'Hour':<10} {'Entries':<12} {'% of Total':>12}")
        print("-" * 36)

        for r in top5:
            hour = r["_id"] + ":00"
            count = r["totalEntries"]
            percent = (count / total_all) * 100
            print(f"{hour:<10} {count:<12} {percent:>10.2f}%")

    def query_2(self):
        print("\nQuery 2: Top 5 Longest Average Stay Duration")

        pipeline = [
            {
                "$addFields": {
                    "timestamp": {
                        "$dateFromString": {
                            "dateString": {"$concat": ["$date", "T", "$time"]}
                        }
                    }
                }
            },
            {"$sort": {"student_id": 1, "timestamp": 1}},
            {
                "$group": {
                    "_id": "$student_id",
                    "events": {
                        "$push": {
                            "type": "$event_type",
                            "time": "$timestamp"
                        }
                    }
                }
            }
        ]

        data = list(self.events.aggregate(pipeline))

        if not data:
            print("No data found.")
            return

        stay_results = []

        for d in data:
            student_id = d["_id"]
            events = d["events"]

            total_duration = 0
            count = 0
            entry_time = None

            for e in events:
                if e["type"] == "ENTRY":
                    entry_time = e["time"]
                elif e["type"] == "EXIT" and entry_time:
                    duration = (e["time"] - entry_time).total_seconds() / 60
                    total_duration += duration
                    count += 1
                    entry_time = None

            if count > 0:
                avg_duration = total_duration / count
                stay_results.append({
                    "student_id": student_id,
                    "avg_duration": avg_duration
                })

        for r in stay_results:
            student = self.students.find_one({"student_id": r["student_id"]})
            r["major"] = student["major"] if student else "N/A"

        stay_results.sort(key=lambda x: x["avg_duration"], reverse=True)
        top5 = stay_results[:5]

        if not top5:
            print("No valid stay duration data.")
            return

        print(f"\n{'Student ID':<12} {'Major':<25} {'Avg Stay (mins)':>18}")
        print("-" * 60)

        for r in top5:
            print(f"{r['student_id']:<12} {r['major']:<25} {round(r['avg_duration'], 2):>18}")

    def run(self):
        self.query_1()
        self.query_2()


if __name__ == "__main__":
    mq = MongoQueries("utils/config.json")
    mq.run()


