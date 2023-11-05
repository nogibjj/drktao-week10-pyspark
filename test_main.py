import unittest
from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import complex_query


class testFDBFunctions(unittest.TestCase):
    def test_extract(self):
        result = extract()
        self.assertEqual(result, "Success", "Failed to extract data from source")

    # def test_load(self):
    # result = load()
    # self.assertEqual(result, "Success", "Failed to load data to DataBricks")

    # def test_complex_query(self):
    # result = complex_query()
    # self.assertEqual(result, "Success", "Failed to run query")


if __name__ == "__main__":
    unittest.main()
