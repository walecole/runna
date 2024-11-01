import unittest
from unittest.mock import patch, mock_open, MagicMock
from pathlib import Path
from datetime import date
from download_json_to_csv import WorkoutImporter

class TestWorkoutImporter(unittest.TestCase):


    @patch('download_json_to_csv.Path.iterdir')
    @patch('builtins.open', new_callable=mock_open, read_data='{"activityId": "1", "userId": "123", "planDetails": {"id": "abc", "planLength": 4}, "workoutId": "work1", "recordType": "run", "weekOfPlan": 2, "unitOfMeasure": "miles"}')
    def test_load_json_files(self, mock_open, mock_iterdir):
        # Setup mock for directory listing
        mock_json_file = MagicMock()
        mock_json_file.is_file.return_value = True
        mock_json_file.suffix = '.json'

        mock_iterdir.return_value = [mock_json_file]

        importer = WorkoutImporter(data_directory='./data')
        
        # Check that combined_data contains data from JSON
        expected_data = [{
            "activityId": "1", "userId": "123", "planDetails": {"id": "abc", "planLength": 4},
            "workoutId": "work1", "recordType": "run", "weekOfPlan": 2, "unitOfMeasure": "miles"
        }]
        self.assertEqual(importer.combined_data, expected_data)

    
    """ More test check for loading """

def main():
    unittest.main(verbosity=2)

if __name__ == '__main__':
    main()

