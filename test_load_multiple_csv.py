import unittest
from unittest.mock import Mock, patch, MagicMock
from google.cloud import bigquery, storage
from load_multiple_csv import load_csv_files_to_bigquery

class TestLoadCSVToBigQuery(unittest.TestCase):
    def setUp(self):
        """Setup common test components"""
        self.bucket_name = "test-bucket"
        self.dataset_id = "test-dataset"

    #@patch('load_multiple_csv.bigquery.LoadJobConfig')    
    @patch('load_multiple_csv.bigquery.Client')
    @patch('load_multiple_csv.storage.Client')
    def test_successful_file_loading(self, mock_storage_client, mock_bigquery_client):
        """Test successful loading of CSV files to BigQuery"""
        # Mock storage components
        mock_bucket = Mock()
        mock_blob1 = Mock()
        mock_blob1.name = "test_file1.csv"
        mock_blob2 = Mock()
        mock_blob2.name = "test_file2.csv"
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        
        # Mock BigQuery components
        mock_dataset = Mock()
        mock_table = Mock()
        mock_job = Mock()
        mock_job.result.return_value = None
        
        mock_bigquery_client.return_value.dataset.return_value = mock_dataset
        mock_dataset.table.return_value = mock_table
        mock_bigquery_client.return_value.load_table_from_uri.return_value = mock_job
        
    
        load_csv_files_to_bigquery(self.bucket_name, self.dataset_id)
        
        # Verify calls
        self.assertEqual(mock_storage_client.return_value.bucket.call_count, 1)
        self.assertEqual(mock_bigquery_client.return_value.load_table_from_uri.call_count, 2)

    
    def test_invalid_bucket_name(self):
        """Test handling of invalid bucket name"""
        with self.assertRaises(Exception):
            load_csv_files_to_bigquery(None, self.dataset_id)
            
    def test_invalid_dataset_id(self):
        """Test handling of invalid dataset ID"""
        with self.assertRaises(Exception):
            load_csv_files_to_bigquery(self.bucket_name, None)


if __name__ == '__main__':
    unittest.main()