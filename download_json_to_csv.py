from pathlib import Path
import csv
import json
from datetime import date
from google.cloud import storage
import os



class WorkoutImporter:

    def __init__(self, data_directory='./data'):
        self.combined_data = []
        self.load_json_files(data_directory)
        self.today = date.today().strftime("%Y-%m-%d")
        self.bucket_name = "runna"


    def load_json_files(self, data_directory):
        """Reads given JSON files in directory and combines json and returns a list of dictionary """
        try:
            path = Path(data_directory)
            for json_file in path.iterdir():
                if json_file.is_file() and json_file.suffix == '.json':
                    with open(json_file, 'r') as json_file:
                        data = json.load(json_file)
                        self.combined_data.append(data)

            return self.combined_data
        except Exception as e:
            print(f"Failed to parse directory {e}")
            return ""

    def to_csv(self, file_name, field_names, rows):
        """Writes rows of flatten json data into csv"""
        with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(rows) 

    def process_activity_data(self):
        """From the JSON file, the activities records are processed, extracted and save to a cloud storage"""
        activities_fields = ["activity_id", "user_id", "plan_id","plan_length","workout_id", 
                             "record_type", "week_of_plan", "unit_of_measure", "processing_time"]
        rows = []
        try:
            for activity_row in self.combined_data:
                #Extract activity, and plan fields
                activities_data = {
                    "activity_id" : activity_row['activityId'], 
                    "user_id":activity_row['userId'], 
                    "plan_id": activity_row['planDetails']['id'],
                    "plan_length":activity_row['planDetails']['planLength'],
                    "workout_id" : activity_row['workoutId'], 
                    "record_type" : activity_row['recordType'], 
                    "week_of_plan":activity_row['weekOfPlan'], 
                    "unit_of_measure": activity_row['unitOfMeasure'],
                    "processing_time": self.today
                }
                rows.append(activities_data)
            self.to_csv('./processed_data/activity_data.csv', activities_fields, rows)
            self.upload_to_gcs(self.bucket_name,'./processed_data/activity_data.csv','activity_data')
        except Exception as e:
            print(f"Failed to process activity data :{e}")


    def process_lap_data(self):
        """From the JSON file, the laps records are processed, extracted and save to a cloud storage"""
        lap_fields = ["activity_id", "lap_order","average_cadence", "average_heart_rate", "average_speed",
                        "distance", "elevation_gain", "max_cadence", "max_elevation", "min_elevation",
                        "max_heart_rate", "min_heart_rate", "max_speed", "moving_time",
                        "start_timestamp", "total_time", "wkt_step_index", "processing_time"]
        rows = []
        try:
            for lap_row in self.combined_data:
                if "laps" in lap_row:
                    #Extract lap fields, and include index to get the lap order
                    for index, sub_lap in enumerate(lap_row['laps']):
                        laps_data = {
                            "activity_id" : lap_row['activityId'],
                            "lap_order": index,
                            "average_cadence": sub_lap.get('averageCadence'),
                            "average_heart_rate": sub_lap.get('averageHeartRate'),
                            "average_speed": sub_lap.get('averageSpeed'),
                            "distance": sub_lap.get('distance'),
                            "elevation_gain": sub_lap.get("elevationGain"),
                            "max_cadence": sub_lap.get("maxCadence"),
                            "max_elevation": sub_lap.get("maxElevation"),
                            "min_elevation": sub_lap.get("minElevation"),
                            "max_heart_rate": sub_lap.get("maxHeartRate"),
                            "min_heart_rate": sub_lap.get("minHeartRate"),
                            "max_speed": sub_lap.get("maxSpeed"),
                            "moving_time": sub_lap.get("movingTime"),
                            "start_timestamp": sub_lap.get("startTimestamp"),
                            "total_time": sub_lap.get("totalTime"),
                            "wkt_step_index": sub_lap.get("wktStepIndex"),
                            "processing_time": self.today
                        }
                        rows.append(laps_data)
            self.to_csv('./processed_data/lap_data.csv', lap_fields, rows)
            self.upload_to_gcs(self.bucket_name,'./processed_data/lap_data.csv','lap_data')
        except Exception as e:
            print(f'Failed to process lap data :{e}')


    def process_waypoint_data(self):
        """From the JSON file, the waypoint records are processed, extracted and save to a cloud storage"""
        waypoint_fields = ["activity_id", "cadence", "distance", "elevation", "heart_rate",
                            "moving_time", "speed", "timestamp", "power", "stride_length",
                            "step_index","lap_index","raw_speed","accuracy","elevation_accuracy",
                            "type", "processing_time"]
        rows = []

        try:
            for wp_row in self.combined_data:
                if "waypoints" in wp_row:
                    for wp_lap in wp_row['waypoints']:
                        #Extract waypoint fields
                        wp_data = {
                            "activity_id" : wp_row['activityId'],
                            "cadence": wp_lap.get('cadence'),
                            "distance": wp_lap.get('distance'),
                            "elevation": wp_lap.get("elevation"),
                            "heart_rate": wp_lap.get("heartRate"),
                            "moving_time": wp_lap.get("movingTime"),
                            "speed": wp_lap.get("speed"),
                            "timestamp": wp_lap.get("timestamp"),
                            "power": wp_lap.get("power"),
                            "stride_length": wp_lap.get("strideLength"),
                            "step_index":wp_lap.get("stepIndex"),
                            "lap_index":wp_lap.get("lapIndex"),
                            "raw_speed":wp_lap.get("rawSpeed"),
                            "accuracy":wp_lap.get("accuracy"),
                            "elevation_accuracy": wp_lap.get("elevationAccuracy"),
                            "type": wp_lap.get("type"),
                            "processing_time": self.today
                        }
                        rows.append(wp_data)
            self.to_csv('./processed_data/waypoint_data.csv', waypoint_fields, rows)
            self.upload_to_gcs(self.bucket_name,'./processed_data/waypoint_data.csv','waypoint_data')
        except Exception as e:
            print(f"Failed to process waypoint data :{e}")


    def process_metadata_data(self):
        """From the JSON file, the workout metadata records are processed, extracted and save to a cloud storage"""
        workout_metadata_fields = ["activity_id","workout_type","run_type","distance",
                                    "current_est_5k_time_secs","planned_workout_date", "processing_time"]
        rows = []
        
        try:
            for metadata_row in self.combined_data:
                if 'plannedWorkoutMetadata' in metadata_row and metadata_row['plannedWorkoutMetadata']:
                    metadata = {
                        "activity_id": metadata_row['activityId'],
                        "workout_type": metadata_row['plannedWorkoutMetadata'].get('workoutType'),
                        "run_type": metadata_row['plannedWorkoutMetadata'].get('runType'),
                        "distance": metadata_row['plannedWorkoutMetadata'].get('distance'),
                        "current_est_5k_time_secs": metadata_row['plannedWorkoutMetadata'].get('currentEst5kTimeInSecs'),
                        "planned_workout_date": metadata_row['plannedWorkoutMetadata'].get("plannedWorkoutDate"),
                        "processing_time": self.today
                    }
                    rows.append(metadata)
            self.to_csv('./processed_data/workout_metadata.csv', workout_metadata_fields, rows)
            self.upload_to_gcs(self.bucket_name,'./processed_data/workout_metadata.csv','workout_metadata')
        except Exception as e:
            print(f"Failed to process workout metadata data :{e}")


    def _parse_step(self, step, activity_id, repeat_value, lap_order):
        """Function that extracts the step and pace column fields
        Return flatten steps data that include paces
        """
        step_data = {
            "activity_id": activity_id,
            "processing_time": self.today,
            "lap_order": lap_order,
            "step_type": step.get('type'),
            "step_order":step.get('stepOrder'),
            "repeat_value": repeat_value,
            "intensity": step.get('intensity'),
            "duration_type": step.get('durationType'),
            "duration_value": step.get('durationValue'),
            "duration_value_type": step.get('durationValueType'),
            "target_type": step.get('targetType'),
            "pace_slow_text": None,
            "pace_slow_mps": None,
            "pace_average_text": None,
            "pace_average_mps": None,
            "pace_fast_text": None,
            "pace_fast_mps": None
        }
        if step.get("paces"):
            if "slow" in step["paces"]:
                #Get pace data and replace the (:) in order to convert it from text to appropiate data type
                step_data["pace_slow_text"] = step["paces"]["slow"].get("text").replace(":",".")
                step_data["pace_slow_mps"] = step["paces"]["slow"].get("mps")
            if "average" in step["paces"]:
                step_data["pace_average_text"] = step["paces"]["average"].get("text").replace(":",".")
                step_data["pace_average_mps"] = step["paces"]["average"].get("mps")
            if "fast" in step["paces"]:
                step_data["pace_fast_text"] = (step["paces"]["fast"].get("text")).replace(":",".")
                step_data["pace_fast_mps"] = step["paces"]["fast"].get("mps")
        return step_data
    
    def process_step_data(self):
        """From the JSON file, the step records are processed, extracted and save to a cloud storage"""
        steps_fields = ["activity_id", "processing_time", "lap_order", "step_type", "step_order", "repeat_value","intensity",
                "duration_type","duration_value","duration_value_type","target_type",
                "pace_slow_text","pace_slow_mps","pace_average_text","pace_average_mps",
                "pace_fast_text","pace_fast_mps"]
        results = []

        for step_row in self.combined_data:
            stepdata = step_row['plannedWorkoutMetadata']
            if 'stepsV2' in stepdata:
                #Get steps data and pace data by checking step['type'] if it is equal to RepeatStep
                for index, step in enumerate(stepdata['stepsV2']):
                    if step['type'] == 'WorkoutStep':
                        results.append(self._parse_step(step, step_row['activityId'], step.get('repeatValue'), index))
                    elif step['type'] == 'WorkoutRepeatStep':
                        parsed_data = [self._parse_step(pace, step_row['activityId'], step.get('repeatValue'), index) for pace in step['steps']]
                        results.extend(parsed_data)

        self.to_csv('./processed_data/step_data.csv', steps_fields, results)
        self.upload_to_gcs(self.bucket_name,'./processed_data/step_data.csv','step_data')

    def upload_to_gcs(self, bucket_name, source_file_path, destination_blob_name):
        """Upload file to Google Cloud Storage for staging"""
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)
        print(f"File {source_file_path} uploaded to {bucket_name}")

    def import_data(self):
        self.process_activity_data()
        self.process_lap_data()
        self.process_metadata_data()
        self.process_waypoint_data()
        self.process_step_data()


def main():
    try:
        importer = WorkoutImporter()
        importer.import_data()

    except Exception as e:
        print(f"Failed to process json files: {e}")


if __name__ == "__main__":
    main()
                


        
