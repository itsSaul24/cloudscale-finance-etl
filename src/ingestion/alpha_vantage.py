"""
Alpha Vantage API client for fetching stock market data
Handles rate limiting, retries, and data formatting
"""

import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests

from src.utils.config import Config
from src.utils.logger import log_error, log_metric, log_start, log_success, logger


class AlphaVantageClient:
    """
    Client for interacting with Alpha Vantage API

    Features:
    - Rate limiting (5 calls per minute)
    - Automatic retries on failure
    - Data validation
    - Error handling
    """

    def __init__(self, api_key: str = None):
        """
        Initialize the Alpha Vantage client

        Args:
            api_key: Alpha Vantage API key
        """
        self.api_key = api_key or Config.ALPHA_VANTAGE_API_KEY
        if not self.api_key:
            raise ValueError("Alpha Vantage API key is required")

        self.base_url = "https://www.alphavantage.co/query"
        self.last_call_time = 0
        self.call_count = 0
        self.rate_limit_window = 60  # 60 seconds
        self.max_calls_per_minute = Config.ALPHA_VANTAGE_RATE_LIMIT

        logger.info(
            f"Initialized Alpha Vantage client with rate limit: "
            f"{self.max_calls_per_minute} calls/minute"
        )

    def _enforce_rate_limit(self):
        """
        Enforce rate limiting to avoid hitting API limits
        Alpha Vantage allows 5 API calls per minute for free tier
        """
        current_time = time.time()

        # Reset counter if we're in a new minute window
        if current_time - self.last_call_time > self.rate_limit_window:
            self.call_count = 0

        # If we've hit the rate limit, wait
        if self.call_count >= self.max_calls_per_minute:
            wait_time = self.rate_limit_window - (current_time - self.last_call_time)
            if wait_time > 0:
                logger.info(f"Rate limit reached. Waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)
                self.call_count = 0

        # Update for this call
        self.call_count += 1
        self.last_call_time = current_time

    def _make_request(self, params: Dict) -> Dict:
        """
        Make HTTP request to Alpha Vantage API with retry logic

        Args:
            params: Query parameters for the API

        Returns:
            JSON response from the API

        Raises:
            requests.RequestException: If request fails after retries
        """
        # Add API key to params
        params["apikey"] = self.api_key

        # Retry logic
        for attempt in range(Config.MAX_RETRIES):
            try:
                # Enforce rate limit before making request
                self._enforce_rate_limit()

                # Make the request
                response = requests.get(
                    self.base_url, params=params, timeout=30  # 30 second timeout
                )

                # Check for HTTP errors
                response.raise_for_status()

                # Parse JSON
                data = response.json()

                # Check for API-specific errors
                if "Error Message" in data:
                    raise ValueError(f"API Error: {data['Error Message']}")

                if "Note" in data:
                    # This usually means we hit the rate limit
                    logger.warning(f"API Note: {data['Note']}")
                    if "call frequency" in data["Note"].lower():
                        # Wait extra time for rate limit
                        time.sleep(60)
                        continue

                return data

            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout on attempt {attempt + 1}")
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(2**attempt)  # Exponential backoff
                else:
                    raise

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed on attempt {attempt + 1}: {e}")
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(2**attempt)
                else:
                    raise

    def get_daily_prices(
        self, symbol: str, outputsize: str = "compact"
    ) -> Optional[pd.DataFrame]:
        """
        Fetch daily price data for a stock symbol

        Args:
            symbol: Stock ticker symbol (e.g., 'AAPL', 'MSFT')
            outputsize: 'compact' (last 100 days) or 'full' (20+ years)

        Returns:
            DataFrame with columns: open, high, low, close, volume
            None if request fails
        """
        log_start(f"Fetching daily prices for {symbol}")

        try:
            # API parameters
            params = {
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol.upper(),
                "outputsize": outputsize,
            }

            # Make API request
            data = self._make_request(params)

            # Extract time series data
            time_series_key = "Time Series (Daily)"
            if time_series_key not in data:
                logger.error(f"No time series data found for {symbol}")
                return None

            # Convert to DataFrame
            time_series = data[time_series_key]
            df = pd.DataFrame.from_dict(time_series, orient="index")

            # Clean column names (remove the numeric prefix)
            df.columns = ["open", "high", "low", "close", "volume"]

            # Convert index to datetime
            df.index = pd.to_datetime(df.index)
            df.index.name = "date"

            # Sort by date (newest first in API response)
            df = df.sort_index(ascending=True)

            # Convert string values to numeric
            numeric_columns = ["open", "high", "low", "close", "volume"]
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            # Add metadata
            df.attrs["symbol"] = symbol.upper()
            df.attrs["last_updated"] = datetime.now()

            log_success(
                f"Fetching daily prices for {symbol}",
                f"Retrieved {len(df)} days of data",
            )
            log_metric(f"records_fetched_{symbol}", len(df))

            return df

        except Exception as e:
            log_error(f"Fetching daily prices for {symbol}", e)
            return None

    def get_intraday_prices(
        self, symbol: str, interval: str = "5min"
    ) -> Optional[pd.DataFrame]:
        """
        Fetch intraday price data for a stock symbol

        Args:
            symbol: Stock ticker symbol
            interval: Time interval - '1min', '5min', '15min', '30min', '60min'

        Returns:
            DataFrame with intraday price data
            None if request fails
        """
        log_start(f"Fetching intraday prices for {symbol}")

        try:
            # Validate interval
            valid_intervals = ["1min", "5min", "15min", "30min", "60min"]
            if interval not in valid_intervals:
                raise ValueError(f"Invalid interval. Must be one of: {valid_intervals}")

            # API parameters
            params = {
                "function": "TIME_SERIES_INTRADAY",
                "symbol": symbol.upper(),
                "interval": interval,
                "outputsize": "compact",  # Last 100 data points
            }

            # Make API request
            data = self._make_request(params)

            # Extract time series data
            time_series_key = f"Time Series ({interval})"
            if time_series_key not in data:
                logger.error(f"No intraday data found for {symbol}")
                return None

            # Convert to DataFrame
            time_series = data[time_series_key]
            df = pd.DataFrame.from_dict(time_series, orient="index")

            # Clean column names
            df.columns = ["open", "high", "low", "close", "volume"]

            # Convert index to datetime
            df.index = pd.to_datetime(df.index)
            df.index.name = "timestamp"

            # Sort by timestamp
            df = df.sort_index(ascending=True)

            # Convert to numeric
            numeric_columns = ["open", "high", "low", "close", "volume"]
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            # Add metadata
            df.attrs["symbol"] = symbol.upper()
            df.attrs["interval"] = interval
            df.attrs["last_updated"] = datetime.now()

            log_success(
                f"Fetching intraday prices for {symbol}",
                f"Retrieved {len(df)} {interval} intervals",
            )

            return df

        except Exception as e:
            log_error(f"Fetching intraday prices for {symbol}", e)
            return None

    def get_company_overview(self, symbol: str) -> Optional[Dict]:
        """
        Fetch company fundamental data

        Args:
            symbol: Stock ticker symbol

        Returns:
            Dictionary with company information
            None if request fails
        """
        log_start(f"Fetching company overview for {symbol}")

        try:
            # API parameters
            params = {"function": "OVERVIEW", "symbol": symbol.upper()}

            # Make API request
            data = self._make_request(params)

            # Check if we got valid data
            if not data or "Symbol" not in data:
                logger.error(f"No company data found for {symbol}")
                return None

            # Extract key fields
            overview = {
                "symbol": data.get("Symbol", ""),
                "name": data.get("Name", ""),
                "description": data.get("Description", ""),
                "exchange": data.get("Exchange", ""),
                "currency": data.get("Currency", ""),
                "country": data.get("Country", ""),
                "sector": data.get("Sector", ""),
                "industry": data.get("Industry", ""),
                "market_cap": pd.to_numeric(
                    data.get("MarketCapitalization", 0), errors="coerce"
                ),
                "pe_ratio": pd.to_numeric(data.get("PERatio", None), errors="coerce"),
                "dividend_yield": pd.to_numeric(
                    data.get("DividendYield", 0), errors="coerce"
                ),
                "52_week_high": pd.to_numeric(
                    data.get("52WeekHigh", None), errors="coerce"
                ),
                "52_week_low": pd.to_numeric(
                    data.get("52WeekLow", None), errors="coerce"
                ),
                "shares_outstanding": pd.to_numeric(
                    data.get("SharesOutstanding", 0), errors="coerce"
                ),
                "last_updated": datetime.now(),
            }

            log_success(f"Fetching company overview for {symbol}")

            return overview

        except Exception as e:
            log_error(f"Fetching company overview for {symbol}", e)
            return None

    def get_technical_indicators(
        self, symbol: str, indicator: str = "SMA", time_period: int = 20
    ) -> Optional[pd.DataFrame]:
        """
        Fetch technical indicator data

        Args:
            symbol: Stock ticker symbol
            indicator: Technical indicator (SMA, EMA, RSI, MACD, etc.)
            time_period: Number of periods for the indicator

        Returns:
            DataFrame with indicator values
            None if request fails
        """
        log_start(f"Fetching {indicator} for {symbol}")

        try:
            # API parameters
            params = {
                "function": indicator.upper(),
                "symbol": symbol.upper(),
                "interval": "daily",
                "time_period": time_period,
                "series_type": "close",
            }

            # Make API request
            data = self._make_request(params)

            # Extract technical analysis data
            ta_key = f"Technical Analysis: {indicator.upper()}"
            if ta_key not in data:
                logger.error(f"No {indicator} data found for {symbol}")
                return None

            # Convert to DataFrame
            ta_data = data[ta_key]
            df = pd.DataFrame.from_dict(ta_data, orient="index")

            # Convert index to datetime
            df.index = pd.to_datetime(df.index)
            df.index.name = "date"

            # Sort by date
            df = df.sort_index(ascending=True)

            # Convert to numeric
            for col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            # Add metadata
            df.attrs["symbol"] = symbol.upper()
            df.attrs["indicator"] = indicator.upper()
            df.attrs["time_period"] = time_period
            df.attrs["last_updated"] = datetime.now()

            log_success(
                f"Fetching {indicator} for {symbol}", f"Retrieved {len(df)} data points"
            )

            return df

        except Exception as e:
            log_error(f"Fetching {indicator} for {symbol}", e)
            return None

    def fetch_multiple_symbols(self, symbols: List[str]) -> Dict[str, pd.DataFrame]:
        """
        Fetch daily price data for multiple symbols

        Args:
            symbols: List of stock ticker symbols

        Returns:
            Dictionary mapping symbol to DataFrame
        """
        results = {}
        total = len(symbols)

        logger.info(f"Fetching data for {total} symbols")

        for i, symbol in enumerate(symbols, 1):
            logger.info(f"Processing {i}/{total}: {symbol}")

            # Fetch data
            df = self.get_daily_prices(symbol)

            if df is not None:
                results[symbol] = df
            else:
                logger.warning(f"Failed to fetch data for {symbol}")

            # Add small delay between symbols to be nice to the API
            if i < total:
                time.sleep(1)

        logger.info(f"Successfully fetched data for {len(results)}/{total} symbols")
        return results


# Example usage and testing
if __name__ == "__main__":
    # Initialize client
    client = AlphaVantageClient()

    # Test fetching daily prices
    print("\n1. Testing daily price fetch...")
    df = client.get_daily_prices("AAPL")
    if df is not None:
        print(f"✓ Fetched {len(df)} days of AAPL data")
        print(f"  Date range: {df.index.min()} to {df.index.max()}")
        print(f"  Latest close: ${df['close'].iloc[-1]:.2f}")
        print("\nFirst 5 rows:")
        print(df.head())

    # Test fetching company overview
    print("\n2. Testing company overview...")
    overview = client.get_company_overview("MSFT")
    if overview:
        print(f"✓ Fetched overview for {overview['name']}")
        print(f"  Sector: {overview['sector']}")
        print(f"  Market Cap: ${overview['market_cap']:,.0f}")

    # Test fetching technical indicator
    print("\n3. Testing technical indicator...")
    sma = client.get_technical_indicators("GOOGL", "SMA", 20)
    if sma is not None:
        print("✓ Fetched SMA(20) for GOOGL")
        print(f"  Latest SMA: ${sma.iloc[-1, 0]:.2f}")

    # Test multiple symbols
    print("\n4. Testing multiple symbols...")
    symbols = ["AAPL", "MSFT", "GOOGL"]
    results = client.fetch_multiple_symbols(symbols)
    print(f"✓ Fetched data for {len(results)} symbols")
