import requests
import pandas as pd
import argparse
from bs4 import BeautifulSoup

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("open_insider_url")
    args = parser.parse_args()

    response = requests.get(args.open_insider_url)
    
    soup = BeautifulSoup(response.content)

    data_table = soup.find_all("table", {
        "class": "tinytable", 
        "width":"100%", 
        "cellpadding":"0",
        "cellspacing": "0",
        "border": "0",
    })[0]

    df: list[pd.DataFrame] = pd.read_html(str(data_table), flavor="bs4")
    df[0].to_csv("./open_insider_info.csv", index=False)