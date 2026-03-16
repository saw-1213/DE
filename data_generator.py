import json
import csv
import random
import uuid
from datetime import datetime, timedelta

class LibraryDatasetGenerator:
    def __init__(self, num_students, total_target_visits, date_str, duration_in_days=5):
        self.num_students = num_students
        self.total_target_visits = total_target_visits
        self.base_date = datetime.fromisoformat(date_str)
        self.duration_in_days = duration_in_days
        self.students = []
        self.events = []
        self.rooms = ["1A", "1B", "1C", "1D", "1E", "1F", "2A", "2B", "2C", "2D", "2E", "2F"]

    def generate_students(self):
        majors = ["Data Science", "Computer Science", "Business Admin", "Mechanical Engineering", "Law"]
        levels = ["Undergraduate", "Postgraduate"]
        for i in range(self.num_students):
            student_id = str(2509600 + i)
            self.students.append({
                "student_id": student_id,
                "major": random.choice(majors),
                "year_of_study": random.randint(1, 4),
                "study_level": random.choices(levels, weights=[0.7, 0.3], k=1)[0]
            })

    def _create_event(self, student_id, event_type, gate_type, location, timestamp):
        return {
            "event_id": str(uuid.uuid4()),
            "student_id": student_id,
            "event_type": event_type,
            "gate_type": gate_type,
            "location": location,
            "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        }

    def _generate_arrival_time(self, visit_date):
        while True:
            hour = random.gauss(12, 2.5)
            if 8.0 <= hour <= 19.5:
                break
        return visit_date + timedelta(hours=hour)

    def generate_events(self):
        unsorted_events = []
        
        for _ in range(self.total_target_visits):
            student_id = random.choice(self.students)["student_id"]
            
            random_day_offset = random.randint(0, self.duration_in_days - 1)
            visit_date = self.base_date + timedelta(days=random_day_offset)
            
            arrival_time = self._generate_arrival_time(visit_date)
            stay_duration = timedelta(minutes=random.randint(30, 120))
            exit_time = arrival_time + stay_duration
            
            closing_time = visit_date + timedelta(hours=20)
            if exit_time > closing_time:
                exit_time = closing_time

            unsorted_events.append(self._create_event(student_id, "ENTRY", "MAIN_GATE", "MAIN_HALL", arrival_time))
            
            uses_room = random.choice([True, False])
            if uses_room:
                room_entry_delay = timedelta(minutes=random.randint(5, 15))
                room_entry_time = arrival_time + room_entry_delay
                
                if room_entry_time < exit_time:
                    room_duration = timedelta(minutes=random.randint(45, 120))
                    room_exit_time = room_entry_time + room_duration
                    
                    if room_exit_time > exit_time:
                        room_exit_time = exit_time - timedelta(minutes=2)
                        
                    if room_exit_time > room_entry_time:
                        room = random.choice(self.rooms)
                        unsorted_events.append(self._create_event(student_id, "ENTRY", "ROOM_GATE", room, room_entry_time))
                        unsorted_events.append(self._create_event(student_id, "EXIT", "ROOM_GATE", room, room_exit_time))

            unsorted_events.append(self._create_event(student_id, "EXIT", "MAIN_GATE", "MAIN_HALL", exit_time))
            
        self.events = sorted(unsorted_events, key=lambda x: x["timestamp"])

    def save_to_json(self, data, filename):
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)

    def save_to_csv(self, data, filename):
        keys = data[0].keys()
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)

def execute_data_generation():
    generator = LibraryDatasetGenerator(num_students=250, total_target_visits=2500, date_str="2026-03-05T00:00:00", duration_in_days=5)
    generator.generate_students()
    generator.generate_events()
    
    generator.save_to_json(generator.students, "students_dataset.json")
    generator.save_to_json(generator.events, "library_events.json")
    generator.save_to_csv(generator.students, "students_dataset.csv")
    generator.save_to_csv(generator.events, "library_events.csv")

if __name__ == "__main__":
    execute_data_generation()