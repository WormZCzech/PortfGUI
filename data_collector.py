import yfinance as yf
import requests
import schedule
import time
from datetime import datetime
import influxdb_client, os
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# API keys
INFLUXDB_API = "YJdH7hSNXunR3th-rSEvSwlnt-yBVIZR9xpq_G4IMzKKjeJch0c5IzoTfOSVzeIzPKqd6Yj54E710Zm-SBB3qg=="


# --- InfluxDB 2.x Setup ---
# Set your InfluxDB 2.x token in the environment variable INFLUXDB_TOKEN
token = INFLUXDB_API
if not token:
    print("Error: INFLUXDB_TOKEN is not set!")
else:
    print("INFLUXDB_TOKEN is set.")
org = "Auret"         # Update as needed
url = "http://127.0.0.1:8086"  # Update if your InfluxDB instance is elsewhere
bucket = "financial_data"     # Name of your bucket in InfluxDB 2.x

# Create InfluxDB client and write API (synchronous)
influx_client = InfluxDBClient(url=url, token=token, org=org)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)



# --- Data Fetching Functions ---

def fetch_stock_price(symbol):
    """
    Fetches the latest stock price for a given symbol using yfinance.
    """
    try:
        ticker = yf.Ticker(symbol)
        # Fetch historical data for today; you can also use .info for a quick price lookup.
        hist = ticker.history(period="1d")
        if hist.empty:
            print(f"No data found for {symbol}.")
            return None
        # Using the latest closing price as the current price
        price = hist['Close'].iloc[-1]
        return float(price)
    except Exception as e:
        print(f"Error fetching data for stock {symbol}: {e}")
        return None

def fetch_crypto_price(crypto_id='bitcoin'):
    """
    Fetches the current price of a cryptocurrency using the CoinGecko API.
    """
    url = f'https://api.coingecko.com/api/v3/simple/price'
    params = {
        'ids': crypto_id,
        'vs_currencies': 'usd'
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        price = data.get(crypto_id, {}).get('usd')
        return float(price) if price else None
    except Exception as e:
        print(f"Error fetching crypto data for {crypto_id}: {e}")
        return None

def fetch_precious_metals():
    """
    Fetches the current prices for gold and silver using yfinance.
    
    Uses the futures tickers:
      - Gold: "GC=F"
      - Silver: "SI=F"
    
    Returns a dictionary with keys 'gold' and 'silver'.
    """
    try:
        # Fetch gold price using the Gold futures ticker
        ticker_gold = yf.Ticker("GC=F")
        gold_hist = ticker_gold.history(period="1d")
        gold_price = gold_hist["Close"].iloc[-1] if not gold_hist.empty else None

        # Fetch silver price using the Silver futures ticker
        ticker_silver = yf.Ticker("SI=F")
        silver_hist = ticker_silver.history(period="1d")
        silver_price = silver_hist["Close"].iloc[-1] if not silver_hist.empty else None

        return {
            "gold": float(gold_price) if gold_price is not None else None,
            "silver": float(silver_price) if silver_price is not None else None
        }
    except Exception as e:
        print("Error fetching precious metals using yfinance:", e)
        return None

def fetch_bond_data():
    """
    Fetches short-term bonds data from the CNB API's daily endpoint:
        https://api.cnb.cz/cnbapi/skd/daily

    The endpoint returns a JSON object with a key "skds" that contains a list of bond objects.
    Each bond object contains fields such as settlementDate, isin, issueCode, issueName,
    nominalValueCZK, averagePriceToValue, and nominalValueOfSettlementCZK.

    This function parses the response and returns a dictionary keyed by issueCode.
    """
    url = "https://api.cnb.cz/cnbapi/skd/daily"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print("Received CNB short-term bonds data:", data)
        
        bond_data = {}
        if isinstance(data, dict) and "skds" in data:
            skds = data["skds"]
            for bond in skds:
                # Use issueCode as key; you could also use issueName if desired.
                key = bond.get("issueCode", "unknown")
                try:
                    avg_price = float(bond.get("averagePriceToValue")) if bond.get("averagePriceToValue") is not None else None
                except Exception:
                    avg_price = None

                bond_data[key] = {
                    "settlementDate": bond.get("settlementDate"),
                    "isin": bond.get("isin"),
                    "issueName": bond.get("issueName"),
                    "nominalValueCZK": bond.get("nominalValueCZK"),
                    "averagePriceToValue": avg_price,
                    "nominalValueOfSettlementCZK": bond.get("nominalValueOfSettlementCZK")
                }
        else:
            print("Unexpected data format for bonds:", data)
        return bond_data

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while fetching short-term bonds data: {http_err}")
    except Exception as e:
        print("Error fetching short-term bonds data from CNB API:", e)
    return None


def fetch_long_term_bonds():
    """
    Fetches long-term bonds (yield curve) data from the CNB API's daily endpoint:
        https://api.cnb.cz/cnbapi/czeonia/daily

    The endpoint returns a JSON object containing a key "czeoniaDaily". For example:
    
        {
          "czeoniaDaily": {
            "validFor": "2025-02-04",
            "volumeInCZKmio": 14347,
            "rate": 3.8
          }
        }
    
    This function parses that data and returns it as a dictionary.
    """
    url = "https://api.cnb.cz/cnbapi/czeonia/daily"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print("Received CNB long-term bonds data:", data)
        print("Type of long-term bonds data:", type(data))
        
        # Check if the expected key exists:
        if isinstance(data, dict) and "czeoniaDaily" in data:
            daily = data["czeoniaDaily"]
            try:
                rate = float(daily.get("rate")) if daily.get("rate") is not None else None
            except Exception:
                rate = None
            long_term_data = {
                "rate": rate,
                "validFor": daily.get("validFor"),
                "volumeInCZKmio": daily.get("volumeInCZKmio")
            }
            return long_term_data
        else:
            print("Unexpected data format for long-term bonds:", data)
        return {}
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while fetching long-term bonds data: {http_err}")
    except Exception as e:
        print("Error fetching long-term bonds data from CNB API:", e)
    return {}


import yfinance as yf
import requests
import schedule
import time
from datetime import datetime

def fetch_stock_price(symbol):
    """
    Fetches the latest stock price for a given symbol using yfinance.
    """
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1d")
        if hist.empty:
            print(f"No data found for {symbol}.")
            return None
        price = hist['Close'].iloc[-1]
        return float(price)
    except Exception as e:
        print(f"Error fetching data for stock {symbol}: {e}")
        return None

def fetch_crypto_price(crypto_id='bitcoin'):
    """
    Fetches the current price of a cryptocurrency using the CoinGecko API.
    """
    url = 'https://api.coingecko.com/api/v3/simple/price'
    params = {'ids': crypto_id, 'vs_currencies': 'usd'}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        price = data.get(crypto_id, {}).get('usd')
        return float(price) if price is not None else None
    except Exception as e:
        print(f"Error fetching crypto data for {crypto_id}: {e}")
        return None

def fetch_precious_metals():
    """
    Fetches the current prices for gold and silver using yfinance.
    
    Uses the futures tickers:
      - Gold: "GC=F"
      - Silver: "SI=F"
    
    Returns a dictionary with keys 'gold' and 'silver'.
    """
    try:
        # Fetch gold price using the Gold futures ticker
        ticker_gold = yf.Ticker("GC=F")
        gold_hist = ticker_gold.history(period="1d")
        gold_price = gold_hist["Close"].iloc[-1] if not gold_hist.empty else None

        # Fetch silver price using the Silver futures ticker
        ticker_silver = yf.Ticker("SI=F")
        silver_hist = ticker_silver.history(period="1d")
        silver_price = silver_hist["Close"].iloc[-1] if not silver_hist.empty else None

        return {
            "gold": float(gold_price) if gold_price is not None else None,
            "silver": float(silver_price) if silver_price is not None else None
        }
    except Exception as e:
        print("Error fetching precious metals using yfinance:", e)
        return None

def fetch_bond_data():
    """
    Fetches short-term bonds data from the CNB API's daily endpoint:
        https://api.cnb.cz/cnbapi/skd/daily

    The endpoint returns a JSON object with a key "skds" that contains a list of bond objects.
    This function parses the response and returns a dictionary keyed by issueCode.
    """
    url = "https://api.cnb.cz/cnbapi/skd/daily"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print("Received CNB short-term bonds data:", data)
        
        bond_data = {}
        if isinstance(data, dict) and "skds" in data:
            skds = data["skds"]
            for bond in skds:
                key = bond.get("issueCode", "unknown")
                try:
                    avg_price = float(bond.get("averagePriceToValue")) if bond.get("averagePriceToValue") is not None else None
                except Exception:
                    avg_price = None
                bond_data[key] = {
                    "settlementDate": bond.get("settlementDate"),
                    "isin": bond.get("isin"),
                    "issueName": bond.get("issueName"),
                    "nominalValueCZK": bond.get("nominalValueCZK"),
                    "averagePriceToValue": avg_price,
                    "nominalValueOfSettlementCZK": bond.get("nominalValueOfSettlementCZK")
                }
        else:
            print("Unexpected data format for short-term bonds:", data)
        return bond_data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while fetching short-term bonds data: {http_err}")
    except Exception as e:
        print("Error fetching short-term bonds data from CNB API:", e)
    return None

def fetch_long_term_bonds():
    """
    Fetches long-term bonds (yield curve) data from the CNB API's daily endpoint:
        https://api.cnb.cz/cnbapi/czeonia/daily

    The endpoint returns a JSON object containing a key "czeoniaDaily". For example:
    
        {
          "czeoniaDaily": {
            "validFor": "2025-02-04",
            "volumeInCZKmio": 14347,
            "rate": 3.8
          }
        }
    
    This function parses that data and returns it as a dictionary.
    """
    url = "https://api.cnb.cz/cnbapi/czeonia/daily"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print("Received CNB long-term bonds data:", data)
        print("Type of long-term bonds data:", type(data))
        
        if isinstance(data, dict) and "czeoniaDaily" in data:
            daily = data["czeoniaDaily"]
            try:
                rate = float(daily.get("rate")) if daily.get("rate") is not None else None
            except Exception:
                rate = None
            long_term_data = {
                "rate": rate,
                "validFor": daily.get("validFor"),
                "volumeInCZKmio": daily.get("volumeInCZKmio")
            }
            return long_term_data
        else:
            print("Unexpected data format for long-term bonds:", data)
        return {}
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while fetching long-term bonds data: {http_err}")
    except Exception as e:
        print("Error fetching long-term bonds data from CNB API:", e)
    return {}

def fetch_exchange_rates():
    """
    Fetches exchange rates for EUR and USD from the Czech National Bank.
    Uses the daily exchange rate text file available at:
      https://www.cnb.cz/en/financial_markets/foreign_exchange_market/exchange_rate_fixing/daily.txt

    The file format is:
      Line 1: Date information
      Line 2: Header ("Country|Currency|Amount|Code|Rate")
      Subsequent lines: Data

    This function extracts rates for EUR and USD, converting comma decimals to dots.
    If the "Amount" is not 1, it adjusts the rate accordingly.
    Returns a dictionary with keys "EUR" and "USD".
    """
    url = "https://www.cnb.cz/en/financial_markets/foreign_exchange_market/exchange_rate_fixing/daily.txt"
    try:
        response = requests.get(url)
        response.raise_for_status()
        text = response.text
        lines = text.splitlines()
        
        rates = {}
        # Data starts from line 3 (index 2)
        for line in lines[2:]:
            parts = line.split("|")
            if len(parts) < 5:
                continue
            code = parts[3].strip()
            if code in ["EUR", "USD"]:
                try:
                    amount = float(parts[2].strip())
                    # Replace comma with dot for decimal conversion
                    rate_str = parts[4].strip().replace(",", ".")
                    rate_val = float(rate_str)
                    rate_per_unit = rate_val / amount
                    rates[code] = rate_per_unit
                except Exception as e:
                    print(f"Error parsing line: {line} - {e}")
        return rates
    except Exception as e:
        print("Error fetching exchange rates from CNB:", e)
        return {}




# --- InfluxDB Writing Function using InfluxDB 2.x Client ---

def write_data_to_influx(data):
    """
    Writes the collected data to InfluxDB 2.x.
    Creates a list of Point objects (one per measurement) and writes them to the specified bucket.
    """
    points = []
    timestamp = data['timestamp']
    
    # Stocks
    for symbol, price in data.get('stocks', {}).items():
        points.append(
            Point("stocks")
            .tag("symbol", symbol)
            .field("price", price if price is not None else 0)
            .time(timestamp, WritePrecision.NS)
        )
    
    # Cryptocurrencies
    for crypto, price in data.get('cryptocurrencies', {}).items():
        points.append(
            Point("cryptocurrencies")
            .tag("crypto", crypto)
            .field("price", price if price is not None else 0)
            .time(timestamp, WritePrecision.NS)
        )
    
    # Precious Metals
    for metal, price in data.get('precious_metals', {}).items():
        points.append(
            Point("precious_metals")
            .tag("metal", metal)
            .field("price", price if price is not None else 0)
            .time(timestamp, WritePrecision.NS)
        )
    
    # Short-Term Bonds (store averagePriceToValue)
    for issue, bond_info in data.get('short_term_bonds', {}).items():
        points.append(
            Point("short_term_bonds")
            .tag("issueCode", issue)
            .tag("issueName", bond_info.get('issueName', 'unknown'))
            .field("averagePriceToValue", bond_info.get("averagePriceToValue") if bond_info.get("averagePriceToValue") is not None else 0)
            .time(timestamp, WritePrecision.NS)
        )
    
    # Long-Term Bonds
    lt = data.get('long_term_bonds', {})
    if lt:
        points.append(
            Point("long_term_bonds")
            .field("rate", lt.get("rate") if lt.get("rate") is not None else 0)
            .field("volumeInCZKmio", lt.get("volumeInCZKmio") if lt.get("volumeInCZKmio") is not None else 0)
            .tag("validFor", lt.get("validFor", "unknown"))
            .time(timestamp, WritePrecision.NS)
        )
    
    # Exchange Rates
    for currency, rate in data.get('exchange_rates', {}).items():
        points.append(
            Point("exchange_rates")
            .tag("currency", currency)
            .field("rate", rate if rate is not None else 0)
            .time(timestamp, WritePrecision.NS)
        )
    
    try:
        write_api.write(bucket=bucket, org=org, record=points)
        print("Data written to InfluxDB successfully.")
    except Exception as e:
        print("Error writing data to InfluxDB:", e)



# --- Data Collection Function ---

def collect_data():
    """
    Collects data from multiple sources and prints it.
    Later, you'll store or forward this data to a database.
    """
    # Example symbols; adjust as needed.
    stock_symbols = ['^GSPC', 'VUN.TO', 'MSFT', 'CEZ.PR']
    crypto_ids = ['bitcoin', 'ethereum']
    
    data = {
        'timestamp': datetime.utcnow().isoformat(),
        'stocks': {},
        'cryptocurrencies': {},
        'precious_metals': fetch_precious_metals(),
        'short_term_bonds': fetch_bond_data(),
        'long_term_bonds': fetch_long_term_bonds(),
        'exchange_rates': fetch_exchange_rates()
    }
    
    for symbol in stock_symbols:
        price = fetch_stock_price(symbol)
        data['stocks'][symbol] = price
        print(f"[{datetime.utcnow().isoformat()}] Stock {symbol}: {price}")
    
    for crypto in crypto_ids:
        price = fetch_crypto_price(crypto)
        data['cryptocurrencies'][crypto] = price
        print(f"[{datetime.utcnow().isoformat()}] Crypto {crypto}: {price}")
    
    # Print precious metals and bonds data
    print(f"[{datetime.utcnow().isoformat()}] Precious Metals: {data['precious_metals']}")
    print(f"[{datetime.utcnow().isoformat()}] Short Term Bonds: {data['short_term_bonds']}")
    print(f"[{datetime.utcnow().isoformat()}] Long Term Bonds: {data['long_term_bonds']}")
    print(f"[{datetime.utcnow().isoformat()}] Exchange Rates (CZK per unit): {data['exchange_rates']}")

    # Write data to InfluxDB
    write_data_to_influx(data)
    
    # Here, you would normally store the data in a database or send it to another service.
    # For now, we simply print it.
    print("Collected data:", data)
    
def main():
    # Schedule the data collection every minute.
    schedule.every(1).minutes.do(collect_data)
    
    print("Starting data collection. Press Ctrl+C to stop.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Data collection stopped.")

if __name__ == "__main__":
    main()