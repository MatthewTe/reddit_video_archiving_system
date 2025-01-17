import requests
import pandas as pd
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("tiingo_api_token")
    parser.add_argument("ticker")

    args = parser.parse_args()

    response = requests.get(
        f"https://api.tiingo.com/tiingo/daily/{args.ticker}/prices",
        params={
            "startDate": "2012-1-1",
            "endDate": "2025-1-10",
            "token": args.tiingo_api_token,
        },
        headers={
            'Content-Type':'application/json'
        }

    )

    df = pd.DataFrame.from_records(response.json())

    df.to_csv(f"{args.ticker}_stock_data.csv", index=False)