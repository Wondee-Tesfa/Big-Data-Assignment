import unittest
from io import StringIO
import pandas as pd
from src.config import TRIP_COLUMNS, DT_FORMAT


class TestLoad(unittest.TestCase):

    def test_buffer_formatting(self):
        """Verifies that DataFrames format correctly into CSV buffers for COPY."""
        data = {col: [1 if "id" in col or "count" in col or "hour" in col or "dow" in col 
                      else (1.0 if "amount" in col or "tax" in col or "extra" in col or "min" in col 
                            or "distance" in col or "longitude" in col or "latitude" in col or "surcharge" in col
                            else ("2015-01-01 12:00:00" if "datetime" in col else "N"))] 
                for col in TRIP_COLUMNS}
        df = pd.DataFrame(data)
        
        buf = StringIO()
        df[TRIP_COLUMNS].to_csv(buf, index=False, header=False, date_format=DT_FORMAT)
        buf.seek(0)
        output = buf.getvalue().strip()
        
        # Buffer must have 1 comma-separated line containing all columns
        self.assertTrue(len(output) > 0)
        self.assertEqual(len(output.split(",")), len(TRIP_COLUMNS))


if __name__ == "__main__":
    unittest.main()
