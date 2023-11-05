from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import complex_query

print("Extracting data...")
extract()

print("Loading data...")
load()

print("Querying data...")
complex_query()
