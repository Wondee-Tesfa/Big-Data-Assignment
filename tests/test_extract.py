import unittest
from pathlib import Path
from src.extract import resolve_csv_path, extract_chunks


class TestExtract(unittest.TestCase):

    def test_resolve_csv_path(self):
        """Verifies that the CSV path resolver identifies the existing data file."""
        csv_path = resolve_csv_path()
        self.assertIsInstance(csv_path, Path)
        self.assertTrue(csv_path.is_file())
        self.assertTrue(csv_path.name.endswith(".csv"))

    def test_extract_chunks_generator(self):
        """Verifies that extract_chunks yields DataFrames of specified chunk size."""
        csv_path = resolve_csv_path()
        chunks = extract_chunks(csv_path, chunk_size=100)
        try:
            first_chunk = next(chunks)
            self.assertEqual(len(first_chunk), 100)
            self.assertIn("VendorID", first_chunk.columns)
            self.assertIn("tpep_pickup_datetime", first_chunk.columns)
        finally:
            chunks.close()


if __name__ == "__main__":
    unittest.main()
