import requests
import datetime
import pandas as pd

API_KEY = ""
SYMBOL = "BAC"
OUTPUT_FILE = "prices.xlsx"


def fetch_all_minute_data(symbol="BAC", ndays=3650):
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(days=ndays)

    start_ts = int(start.timestamp() * 1000)
    end_ts = int(end.timestamp() * 1000)

    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/"
        f"{start_ts}/{end_ts}"
        f"?adjusted=false&sort=asc&limit=50000&apiKey={API_KEY}"
    )

    all_results = []

    while True:
        r = requests.get(url)
        data = r.json()

        if "results" not in data:
            break

        all_results.extend(data["results"])

        if "next_url" not in data:
            break

        url = data["next_url"] + f"&apiKey={API_KEY}"

    return all_results


def clean_to_single_day_prices(raw_rows):
    df = pd.DataFrame(raw_rows)
    df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True)
    df["timestamp"] = df["timestamp"].dt.tz_convert("America/New_York")

    df["date"] = df["timestamp"].dt.date
    df["time"] = df["timestamp"].dt.time

    start = pd.to_datetime("09:30").time()
    end = pd.to_datetime("16:00").time()

    df = df[(df["time"] >= start) & (df["time"] < end)]

    result = {}

    for date, group in df.groupby("date"):
        group = group.sort_values("timestamp").reset_index(drop=True)

        prices = group["c"].values.tolist()

        if len(prices) == 390:
            result[str(date)] = prices

    return result


def build_excel(price_dict):
    df = pd.DataFrame(price_dict)
    df.index = range(1, 391)
    df.to_excel(OUTPUT_FILE)


def main():
    raw = fetch_all_minute_data(SYMBOL, ndays=3650)
    prices = clean_to_single_day_prices(raw)
    build_excel(prices)


if __name__ == "__main__":
    main()
