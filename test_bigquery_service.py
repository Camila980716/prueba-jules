import unittest
from unittest.mock import patch, MagicMock, ANY
import os

# Assuming google.cloud.bigquery types are needed for assertions
from google.cloud import bigquery 
# If bigquery.ScalarQueryParameter etc. are not directly comparable, 
# we might need to inspect their attributes.

# Function to be tested
from bigquery_service import insert_or_update_patient_data

# Define dummy env vars for tests
TEST_PROJECT_ID = "test-project"
TEST_DATASET_ID = "test-dataset"
TEST_TABLE_ID = "test-table"
FULL_TABLE_ID = f"{TEST_PROJECT_ID}.{TEST_DATASET_ID}.{TEST_TABLE_ID}"

class TestBigQueryService(unittest.TestCase):

    @patch.dict(os.environ, {
        "PROJECT_ID": TEST_PROJECT_ID,
        "DATASET_ID": TEST_DATASET_ID,
        "TABLE_ID": TEST_TABLE_ID,
    })
    @patch('bigquery_service.bigquery.Client')
    def test_insert_new_patient(self, MockBigQueryClient):
        mock_client_instance = MockBigQueryClient.return_value
        mock_client_instance.query.return_value.result = MagicMock() # Mock the result() call

        paciente_data = {
            "paciente_clave": "PN001",
            "nombre": "John Doe",
            "edad": 30,
            "ultima_visita": "2023-10-26", # Example of another field
            "prescripciones": [
                {"medicamento": "MedA", "dosis": "10mg", "fecha": "2023-10-26"},
                {"medicamento": "MedB", "dosis": "5mg", "fecha": "2023-10-26"},
            ]
        }

        insert_or_update_patient_data(paciente_data)

        mock_client_instance.query.assert_called_once()
        call_args = mock_client_instance.query.call_args
        
        # Check the query string (first positional argument)
        actual_query = call_args[0][0]
        self.assertIn(f"MERGE `{FULL_TABLE_ID}` AS target_table", actual_query)
        self.assertIn("USING (SELECT @paciente_clave AS K_paciente_clave) AS source_table", actual_query)
        self.assertIn("ON target_table.paciente_clave = source_table.K_paciente_clave", actual_query)
        self.assertIn("WHEN NOT MATCHED BY TARGET THEN", actual_query)
        self.assertIn("INSERT (paciente_clave, nombre, edad, ultima_visita, prescripciones)", actual_query) # Order matters for this simple check
        self.assertIn("VALUES (@paciente_clave, @nombre, @edad, @ultima_visita, @prescripciones)", actual_query)
        
        # Check the JobConfig and its query_parameters (passed as keyword argument)
        job_config = call_args[1]['job_config']
        self.assertIsInstance(job_config, bigquery.QueryJobConfig)
        
        params_dict = {p.name: (p.value, p.parameter_type) for p in job_config.query_parameters}

        self.assertEqual(params_dict["paciente_clave"][0], "PN001")
        self.assertEqual(params_dict["nombre"][0], "John Doe")
        self.assertEqual(params_dict["edad"][0], 30)
        self.assertEqual(params_dict["ultima_visita"][0], "2023-10-26")
        
        # Check prescripciones parameter
        presc_param_value = params_dict["prescripciones"][0]
        presc_param_type = params_dict["prescripciones"][1] # This is the StructType
        
        expected_presc_data = [
            ("MedA", "10mg", "2023-10-26"),
            ("MedB", "5mg", "2023-10-26"),
        ]
        self.assertEqual(presc_param_value, expected_presc_data)
        
        self.assertIsInstance(presc_param_type, bigquery.StructType)
        field_names = [f.name for f in presc_param_type.fields]
        self.assertEqual(field_names, ["medicamento", "dosis", "fecha"])

    @patch.dict(os.environ, {
        "PROJECT_ID": TEST_PROJECT_ID,
        "DATASET_ID": TEST_DATASET_ID,
        "TABLE_ID": TEST_TABLE_ID,
    })
    @patch('bigquery_service.bigquery.Client')
    def test_update_existing_patient_with_prior_prescriptions(self, MockBigQueryClient):
        mock_client_instance = MockBigQueryClient.return_value
        mock_client_instance.query.return_value.result = MagicMock()

        paciente_data = {
            "paciente_clave": "PX007",
            "nombre": "Jane Smith", # Assume name can change
            "edad": 45,             # Assume age can change
            "prescripciones": [ # New prescriptions to add
                {"medicamento": "MedC", "dosis": "100mg", "fecha": "2023-11-01"},
            ]
        }

        insert_or_update_patient_data(paciente_data)
        mock_client_instance.query.assert_called_once()
        call_args = mock_client_instance.query.call_args
        actual_query = call_args[0][0]

        self.assertIn(f"MERGE `{FULL_TABLE_ID}` AS target_table", actual_query)
        self.assertIn("WHEN MATCHED THEN", actual_query)
        # Check for dynamic fields in SET, excluding paciente_clave
        self.assertIn("SET nombre = @nombre, edad = @edad, prescripciones = ARRAY(SELECT DISTINCT x FROM UNNEST(ARRAY_CONCAT(target_table.prescripciones, @prescripciones)) x)", actual_query)
        # A more robust check for SET clause:
        # expected_set_clause = "SET nombre = @nombre, edad = @edad, prescripciones = ARRAY(SELECT DISTINCT x FROM UNNEST(ARRAY_CONCAT(target_table.prescripciones, @prescripciones)) x)"
        # self.assertTrue(expected_set_clause in actual_query or 
        #                 "SET edad = @edad, nombre = @nombre, prescripciones = ARRAY(SELECT DISTINCT x FROM UNNEST(ARRAY_CONCAT(target_table.prescripciones, @prescripciones)) x)" in actual_query)


        job_config = call_args[1]['job_config']
        params_dict = {p.name: (p.value, p.parameter_type) for p in job_config.query_parameters}

        self.assertEqual(params_dict["paciente_clave"][0], "PX007")
        self.assertEqual(params_dict["nombre"][0], "Jane Smith")
        self.assertEqual(params_dict["edad"][0], 45)
        
        presc_param_value = params_dict["prescripciones"][0]
        expected_presc_data = [
            ("MedC", "100mg", "2023-11-01"),
        ]
        self.assertEqual(presc_param_value, expected_presc_data)

    # Test for updating a patient who initially has no prescriptions (empty or null array)
    # This is covered by the general update logic if target_table.prescripciones is NULL (ARRAY_CONCAT handles NULLs gracefully)
    # but a specific test case can ensure clarity.
    @patch.dict(os.environ, {
        "PROJECT_ID": TEST_PROJECT_ID,
        "DATASET_ID": TEST_DATASET_ID,
        "TABLE_ID": TEST_TABLE_ID,
    })
    @patch('bigquery_service.bigquery.Client')
    def test_update_existing_patient_no_prior_prescriptions(self, MockBigQueryClient):
        mock_client_instance = MockBigQueryClient.return_value
        mock_client_instance.query.return_value.result = MagicMock()

        paciente_data = {
            "paciente_clave": "PX008",
            "nombre": "Alice Wonderland",
            "edad": 30,
            "prescripciones": [
                {"medicamento": "MedX", "dosis": "10mg", "fecha": "2023-11-05"},
            ]
        }
        insert_or_update_patient_data(paciente_data)
        mock_client_instance.query.assert_called_once()
        call_args = mock_client_instance.query.call_args
        actual_query = call_args[0][0]

        self.assertIn("WHEN MATCHED THEN", actual_query)
        self.assertIn("prescripciones = ARRAY(SELECT DISTINCT x FROM UNNEST(ARRAY_CONCAT(target_table.prescripciones, @prescripciones)) x)", actual_query)

        job_config = call_args[1]['job_config']
        params_dict = {p.name: (p.value, p.parameter_type) for p in job_config.query_parameters}
        self.assertEqual(params_dict["paciente_clave"][0], "PX008")
        presc_param_value = params_dict["prescripciones"][0]
        expected_presc_data = [("MedX", "10mg", "2023-11-05")]
        self.assertEqual(presc_param_value, expected_presc_data)


    @patch.dict(os.environ, {
        "PROJECT_ID": TEST_PROJECT_ID,
        "DATASET_ID": TEST_DATASET_ID,
        "TABLE_ID": TEST_TABLE_ID,
    })
    @patch('bigquery_service.bigquery.Client')
    def test_update_existing_patient_with_duplicate_prescriptions(self, MockBigQueryClient):
        # This test focuses on the query construction; the actual de-duplication happens in BigQuery
        # We verify that @prescripciones parameter contains all passed items, including potential duplicates.
        mock_client_instance = MockBigQueryClient.return_value
        mock_client_instance.query.return_value.result = MagicMock()

        paciente_data = {
            "paciente_clave": "PX009",
            "nombre": "Bob The Builder",
            "edad": 50,
            "prescripciones": [
                {"medicamento": "MedY", "dosis": "20mg", "fecha": "2023-11-10"}, # New
                {"medicamento": "MedZ", "dosis": "5mg", "fecha": "2023-11-10"},  # New
                {"medicamento": "MedY", "dosis": "20mg", "fecha": "2023-11-10"}, # Duplicate of new
            ]
        }
        insert_or_update_patient_data(paciente_data)
        mock_client_instance.query.assert_called_once()
        call_args = mock_client_instance.query.call_args
        actual_query = call_args[0][0]
        self.assertIn("WHEN MATCHED THEN", actual_query)
        self.assertIn("prescripciones = ARRAY(SELECT DISTINCT x FROM UNNEST(ARRAY_CONCAT(target_table.prescripciones, @prescripciones)) x)", actual_query)

        job_config = call_args[1]['job_config']
        params_dict = {p.name: (p.value, p.parameter_type) for p in job_config.query_parameters}
        
        presc_param_value = params_dict["prescripciones"][0]
        expected_presc_data_in_param = [ # The parameter should contain what was passed
            ("MedY", "20mg", "2023-11-10"),
            ("MedZ", "5mg", "2023-11-10"),
            ("MedY", "20mg", "2023-11-10"),
        ]
        self.assertEqual(presc_param_value, expected_presc_data_in_param)
        # The SQL query `ARRAY(SELECT DISTINCT ...)` is responsible for the final de-duplication in BQ.

    @patch.dict(os.environ, { # Ensure env vars are set for this test too
        "PROJECT_ID": TEST_PROJECT_ID,
        "DATASET_ID": TEST_DATASET_ID,
        "TABLE_ID": TEST_TABLE_ID,
    })
    def test_error_handling_missing_paciente_clave(self):
        paciente_data_no_clave = {
            "nombre": "Missing Clave",
            "edad": 99,
            "prescripciones": []
        }
        with self.assertRaisesRegex(ValueError, "paciente_clave is missing from input and is required for MERGE."):
            insert_or_update_patient_data(paciente_data_no_clave)

    @patch.dict(os.environ, {
        "PROJECT_ID": TEST_PROJECT_ID,
        "DATASET_ID": TEST_DATASET_ID,
        "TABLE_ID": TEST_TABLE_ID,
    })
    @patch('bigquery_service.bigquery.Client')
    def test_bigquery_api_error(self, MockBigQueryClient):
        mock_client_instance = MockBigQueryClient.return_value
        # Simulate an error during the query execution
        mock_client_instance.query.return_value.result.side_effect = Exception("Simulated BigQuery API Error")

        paciente_data = {
            "paciente_clave": "ERR001",
            "nombre": "Error Prone",
            "edad": 50,
            "prescripciones": []
        }
        
        with self.assertRaisesRegex(Exception, "Error during MERGE operation: Simulated BigQuery API Error"):
            insert_or_update_patient_data(paciente_data)

if __name__ == '__main__':
    unittest.main()
