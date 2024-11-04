import unittest
from unittest.mock import patch, mock_open, MagicMock
from pathlib import Path
import json
import csv
from datetime import date
from workout_importer import WorkoutImporter 

class TestWorkoutImporter(unittest.TestCase):
    def setUp(self):
        self.importer = WorkoutImporter(data_directory='./data')
        self.importer.combined_data = [
            {
                "activityId": "1",
                "userId": "user1",
                "planDetails": {"id": "plan1", "planLength": 4},
                "workoutId": "workout1",
                "recordType": "type1",
                "weekOfPlan": 1,
                "unitOfMeasure": "metric",
                "laps": [
                    {
                        "averageCadence": 80,
                        "averageHeartRate": 120,
                        "averageSpeed": 10,
                        "distance": 5,
                        "elevationGain": 50,
                        "maxCadence": 90,
                        "maxElevation": 100,
                        "minElevation": 10,
                        "maxHeartRate": 130,
                        "minHeartRate": 110,
                        "maxSpeed": 12,
                        "movingTime": 30,
                        "startTimestamp": "1723368932948",
                        "totalTime": 35,
                        "wktStepIndex": 1
                    }
                ],
                "waypoints": [
                    {
                        "cadence": 80,
                        "distance": 1,
                        "elevation": 10,
                        "heartRate": 120,
                        "movingTime": 5,
                        "speed": 10,
                        "timestamp": "1723368932948",
                        "power": 200,
                        "strideLength": 1,
                        "stepIndex": 1,
                        "lapIndex": 1,
                        "rawSpeed": 10,
                        "accuracy": 1,
                        "elevationAccuracy": 1,
                        "type": "type1"
                    }
                ],
                "plannedWorkoutMetadata": {
                    "workoutType": "run",
                    "runType": "easy",
                    "distance": 5,
                    "currentEst5kTimeInSecs": 1500,
                    "plannedWorkoutDate": "2021-01-01",
                    "stepsV2": [
                        {
                            "type": "WorkoutStep",
                            "stepOrder": 1,
                            "repeatValue": 1,
                            "intensity": "easy",
                            "durationType": "time",
                            "durationValue": 30,
                            "durationValueType": "seconds",
                            "targetType": "pace",
                            "paces": {
                                "slow": {"text": "5:00", "mps": 3.33},
                                "average": {"text": "4:30", "mps": 3.70},
                                "fast": {"text": "4:00", "mps": 4.17}
                            }
                        }
                    ]
                }
            }
        ]

    @patch('builtins.open', new_callable=mock_open, read_data=json.dumps({
        "activityId": "1",
        "userId": "user1",
        "planDetails": {"id": "plan1", "planLength": 4},
        "workoutId": "workout1",
        "recordType": "type1",
        "weekOfPlan": 1,
        "unitOfMeasure": "metric",
        "laps": [
            {
                "averageCadence": 80,
                "averageHeartRate": 120,
                "averageSpeed": 10,
                "distance": 5,
                "elevationGain": 50,
                "maxCadence": 90,
                "maxElevation": 100,
                "minElevation": 10,
                "maxHeartRate": 130,
                "minHeartRate": 110,
                "maxSpeed": 12,
                "movingTime": 30,
                "startTimestamp": "1723368932948",
                "totalTime": 35,
                "wktStepIndex": 1
            }
        ],
        "waypoints": [
            {
                "cadence": 80,
                "distance": 1,
                "elevation": 10,
                "heartRate": 120,
                "movingTime": 5,
                "speed": 10,
                "timestamp": "1723368932948",
                "power": 200,
                "strideLength": 1,
                "stepIndex": 1,
                "lapIndex": 1,
                "rawSpeed": 10,
                "accuracy": 1,
                "elevationAccuracy": 1,
                "type": "type1"
            }
        ],
        "plannedWorkoutMetadata": {
            "workoutType": "run",
            "runType": "easy",
            "distance": 5,
            "currentEst5kTimeInSecs": 1500,
            "plannedWorkoutDate": "2021-01-01",
            "stepsV2": [
                {
                    "type": "WorkoutStep",
                    "stepOrder": 1,
                    "repeatValue": 1,
                    "intensity": "easy",
                    "durationType": "time",
                    "durationValue": 30,
                    "durationValueType": "seconds",
                    "targetType": "pace",
                    "paces": {
                        "slow": {"text": "5:00", "mps": 3.33},
                        "average": {"text": "4:30", "mps": 3.70},
                        "fast": {"text": "4:00", "mps": 4.17}
                    }
                }
            ]
        }
    }))
    @patch('pathlib.Path.iterdir')
    def test_load_json_files(self, mock_iterdir, mock_open):
        mock_json_file = MagicMock()
        mock_json_file.is_file.return_value = True
        mock_json_file.suffix = '.json'
        mock_iterdir.return_value = [mock_json_file]

        result = self.importer.load_json_files('dummy_directory')

        # Update expected data to match the combined_data structure in setUp
        expected_data = self.importer.combined_data
        self.assertEqual(result, expected_data)
        self.assertEqual(self.importer.combined_data, expected_data)
        mock_open.assert_called_once_with(mock_json_file, 'r')

    @patch('builtins.open', new_callable=mock_open)
    @patch('csv.DictWriter')
    def test_to_csv(self, mock_dict_writer, mock_open):
        mock_writer_instance = mock_dict_writer.return_value
        field_names = ["field1", "field2"]
        rows = [{"field1": "value1", "field2": "value2"}]

        self.importer.to_csv('dummy.csv', field_names, rows)

        mock_open.assert_called_with('dummy.csv', 'w', newline='', encoding='utf-8')
        mock_dict_writer.assert_called_once_with(mock_open(), fieldnames=field_names)
        mock_writer_instance.writeheader.assert_called_once()
        mock_writer_instance.writerows.assert_called_once_with(rows)

    @patch('workout_importer.WorkoutImporter.to_csv')
    @patch('workout_importer.WorkoutImporter.upload_to_gcs')  
    def test_process_activity_data(self, mock_upload_to_gcs, mock_to_csv):
        self.importer.process_activity_data()
        mock_to_csv.assert_called_once()
        mock_upload_to_gcs.assert_called_once_with(self.importer.bucket_name, './processed_data/activity_data.csv', 'activity_data')

    @patch('workout_importer.WorkoutImporter.to_csv')
    @patch('workout_importer.WorkoutImporter.upload_to_gcs')  
    def test_process_lap_data(self, mock_upload_to_gcs, mock_to_csv):
        self.importer.process_lap_data()
        mock_to_csv.assert_called_once()
        mock_upload_to_gcs.assert_called_once_with(self.importer.bucket_name, './processed_data/lap_data.csv', 'lap_data')

    @patch('workout_importer.WorkoutImporter.to_csv')
    @patch('workout_importer.WorkoutImporter.upload_to_gcs')  
    def test_process_waypoint_data(self, mock_upload_to_gcs, mock_to_csv):
        self.importer.process_waypoint_data()
        mock_to_csv.assert_called_once()
        mock_upload_to_gcs.assert_called_once_with(self.importer.bucket_name, './processed_data/waypoint_data.csv', 'waypoint_data')

    @patch('workout_importer.WorkoutImporter.to_csv')
    @patch('workout_importer.WorkoutImporter.upload_to_gcs')  
    def test_process_metadata_data(self, mock_upload_to_gcs, mock_to_csv):
        self.importer.process_metadata_data()
        mock_to_csv.assert_called_once()
        mock_upload_to_gcs.assert_called_once_with(self.importer.bucket_name, './processed_data/workout_metadata.csv', 'workout_metadata')

    @patch('workout_importer.WorkoutImporter.to_csv')
    @patch('workout_importer.WorkoutImporter.upload_to_gcs')  
    def test_process_step_data(self, mock_upload_to_gcs, mock_to_csv):
        self.importer.process_step_data()
        mock_to_csv.assert_called_once()
        mock_upload_to_gcs.assert_called_once_with(self.importer.bucket_name, './processed_data/step_data.csv', 'step_data')

if __name__ == '__main__':
    unittest.main()