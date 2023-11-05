"""
Extract a dataset from a URL like Kaggle or data.gov. JSON or CSV formats tend to work well
"""
import requests

def extract(url1 = "https://raw.githubusercontent.com/fivethirtyeight/data/master/terrorism/eu_terrorism_fatalities_by_country.csv",
            file_path1 = "data/fatalities_by_country.csv",
            url2 = "https://raw.githubusercontent.com/fivethirtyeight/data/master/terrorism/eu_terrorism_fatalities_by_year.csv",
            file_path2 = "data/fatalities_by_year.csv"):
    """"Extract a url to a file path"""
    with requests.get(url1, timeout = 10) as r:
        with open(file_path1, 'wb') as f:
            f.write(r.content)
    
    with requests.get(url2, timeout = 10) as r:
        with open(file_path2, 'wb') as f:
            f.write(r.content)
    return "Success"

