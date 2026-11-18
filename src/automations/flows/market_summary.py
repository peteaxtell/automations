"""Measure index CFD moves using underlying-market and LSE closing times.

Underlying-market closes provide familiar close-to-close performance and the
move since that market last closed. LSE closes provide the corresponding
reference for an LSE-listed ETF: its price already reflects underlying moves
up to the London close, including when the underlying market closes later.

All sampled prices are CFD ask prices, not ETF or cash-index closing prices.
The CFD move since the LSE close is an indicative input to an ETF opening-value
estimate; ETF prices, FX, hedging and tracking differences are not modelled.
"""

import logging
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from time import sleep

import pandas_market_calendars as mcal
import polars as pl
from jinja2 import Environment, FileSystemLoader, select_autoescape
from prefect import flow
from prefect.blocks.system import Secret
from pydantic import SecretStr

from automations.shared.mail import send_mail
from axt_ig_trade import IGRestClient, Resolution

logger = logging.getLogger(__name__)

INSTRUMENTS = [
    {
        "market": "DAX",
        "epic": "IX.D.DAX.FWS1.IP",
        "exchange": "XETR",
    },
    {
        "market": "FTSE 100",
        "epic": "IX.D.FTSE.FWS1.IP",
        "exchange": "LSE",
    },
    {
        "market": "NASDAQ",
        "epic": "IX.D.NASDAQ.FWS1.IP",
        "exchange": "NASDAQ",
    },
    {
        "market": "S&P 500",
        "epic": "IX.D.SPTRD.FWS1.IP",
        "exchange": "NYSE",
    },
]


def get_latest_complete_session_date(exchange: str) -> date:
    """Return the date of the latest complete session for the given exchange.

    Args:
        exchange: Exchange calendar name, for example `LSE` or `NYSE`.

    Returns:
        The date of the latest complete session.
    """
    today = datetime.now(UTC).date()
    close_times = get_session_close_times(exchange, today - timedelta(days=7), today)
    latest_close_time = max([ct for ct in close_times if ct < datetime.now(UTC)])
    return latest_close_time.date()


@lru_cache
def get_session_close_times(exchange: str, start: date, end: date) -> list[datetime]:
    """Return UTC session close times for an inclusive range of session dates.

    Args:
        exchange: Exchange calendar name, for example `LSE` or `NYSE`.
        start: First session date to include.
        end: Last session date to include.

    Returns:
        UTC session close times in chronological order.
    """
    calendar_df = mcal.get_calendar(exchange).schedule(start_date=start, end_date=end)
    return calendar_df["market_close"].tolist()


@lru_cache
def calculate_candle_times(exchange: str, start: date, end: date) -> list[datetime]:
    """Return UTC start times of 30-minute candles ending at session close.

    Include sessions from `start` through `end` and the preceding session,
    whose close provides the reference price for the first return.

    Args:
        exchange: Exchange calendar name, for example `LSE` or `NYSE`.
        start: First session date for which a return is required.
        end: Last session date to include.

    Returns:
        Chronologically ordered candle start times, 30 minutes before each close.
    """
    close_times = get_session_close_times(exchange, start, end).copy()

    # include 7 days prior to start date to ensure previous session is captured which
    # is required to calculate return for start date

    previous_close_time = get_session_close_times(
        exchange, start - timedelta(days=7), start - timedelta(days=1)
    )[-1]

    close_times.append(previous_close_time)

    candle_start_times = [
        close_time - timedelta(minutes=30) for close_time in close_times
    ]
    return sorted(candle_start_times)


def fetch_close_prices_by_session(
    epic: str,
    exchange: str,
    start: date,
    end: date,
    *,
    client: IGRestClient,
    include_latest: bool = False,
) -> pl.DataFrame:
    """Fetch CFD ask prices at the supplied exchange's session closing times.

    Include the preceding session's close as a reference price and optionally
    append the latest ask quote. Skip historical candles that are unavailable.

    Args:
        epic: The epic identifier for the instrument.
        exchange: Reference exchange calendar, which can differ from the underlying exchange.
        start: First session date for which a return is required.
        end: Last session date to include.
        client: IG REST client used to fetch prices.
        include_latest: Whether to append the latest available ask quote.

    Returns:
        A Polars DataFrame containing columns:
            - `epic`
            - `exchange`
            - `time_utc`: Historical candle end time in UTC, or the latest quote's timestamp.
            - `close_price`: Historical closing ask price or latest offer.
    """
    close_prices = []

    candle_times = calculate_candle_times(exchange, start, end)

    logger.info("Getting 30 minute OHLC data for %s for %s", epic, candle_times)

    for candle_start in candle_times:
        ohlc_candles = client.fetch_historic_prices(
            epic,
            Resolution.MINUTE_30,
            from_date=candle_start,
            to_date=candle_start,  # only need one candle so from_date and to_date are the same
        )

        if not ohlc_candles:
            continue

        close_time = datetime.fromisoformat(
            ohlc_candles[0]["snapshotTimeUTC"].replace("/", "-")
        ) + timedelta(minutes=30)

        close_prices.append(
            {
                "epic": epic,
                "exchange": exchange,
                "time_utc": close_time.strftime("%Y/%m/%d %H:%M:%S"),
                "close_price": ohlc_candles[0]["closePrice"]["ask"],
            }
        )

    if include_latest and (latest_price := client.fetch_latest_price(epic)):
        close_prices.append(
            {
                "epic": epic,
                "exchange": exchange,
                "time_utc": datetime.now(UTC).date().strftime("%Y/%m/%d")
                + " "
                + latest_price["updateTime"],
                "close_price": latest_price["offer"],
            }
        )

    return pl.DataFrame(close_prices)


def calculate_returns_by_session(
    epic: str,
    exchange: str,
    start: date,
    end: date,
    *,
    client: IGRestClient,
    include_latest: bool = True,
) -> pl.DataFrame:
    """Calculate close-to-close CFD ask-price returns and an optional since-close return.

    Use the supplied exchange calendar, which can differ from the instrument's
    underlying exchange. Retain the initial reference-price row with a null
    return. If requested and available, append the return from the last available
    session close to the latest quote.

    Args:
        epic: The epic identifier for the instrument.
        exchange: Reference exchange calendar, for example `LSE` or `NYSE`.
        start: First session date for which a return is required.
        end: Last session date to include.
        client: IG REST client used to fetch prices.
        include_latest: Whether to include the latest quote and its since-close return.

    Returns:
        A Polars DataFrame containing columns:
            - `epic`
            - `exchange`
            - `time_utc`
            - `close_price`
            - `session_return_pct`: Percentage change from the preceding available price.
    """
    close_prices_df = fetch_close_prices_by_session(
        epic,
        exchange,
        start,
        end,
        client=client,
        include_latest=include_latest,
    )
    close_prices_df = close_prices_df.sort("time_utc", descending=False)

    return close_prices_df.with_columns(
        (pl.col("close_price").pct_change() * 100).alias("session_return_pct")
    )


def get_returns_data() -> pl.DataFrame:
    """Fetch close-to-close and latest since-close CFD returns for all configured instruments.

    Use each instrument's underlying exchange calendar and, where different,
    the LSE calendar. Add the market name and exclude rows with null returns.

    Returns:
        A Polars DataFrame containing returns data for all instruments.
    """
    returns_dfs: list[pl.DataFrame] = []

    ig_credentials = Secret.load("ig-api-credentials").get()

    client = IGRestClient(
        api_key=SecretStr(ig_credentials["api_key"]),
        username=ig_credentials["username"],
        password=SecretStr(ig_credentials["password"]),
    )

    for instrument in INSTRUMENTS:
        market = pl.lit(instrument["market"]).alias("market")

        underlying_session_returns_df = calculate_returns_by_session(
            epic=instrument["epic"],
            exchange=instrument["exchange"],
            start=get_latest_complete_session_date(instrument["exchange"]),
            end=datetime.now(UTC).date(),
            client=client,
            include_latest=True,
        )

        underlying_session_returns_df = underlying_session_returns_df.with_columns(
            market
        )
        returns_dfs.append(underlying_session_returns_df)

        if instrument["exchange"] != "LSE":
            lse_returns_df = calculate_returns_by_session(
                epic=instrument["epic"],
                exchange="LSE",
                start=get_latest_complete_session_date("LSE"),
                end=datetime.now(UTC).date(),
                client=client,
                include_latest=True,
            )

            lse_returns_df = lse_returns_df.with_columns(market)
            returns_dfs.append(lse_returns_df)
        sleep(3)

    return pl.concat(returns_dfs).filter(pl.col("session_return_pct").is_not_null())


def aggregate_by_market_exchange(returns_df: pl.DataFrame) -> pl.DataFrame:
    """Summarise completed-session and since-close returns by market and reference exchange.

    Each group must contain two chronologically ordered return rows: the latest
    completed-session return followed by the return to the latest quote.

    Args:
        returns_df: A Polars DataFrame containing returns data for all instruments.

    Returns:
        A Polars DataFrame with columns:
            - `market`: The market of the instrument.
            - `exchange`: Reference exchange calendar, not necessarily the underlying exchange.
            - `previous_session_return_pct`: Percentage return from the preceding session close
            to the latest completed session close.
            - `previous_session_time_utc`: Closing timestamp of the latest completed session.
            - `return_since_previous_close_pct`: Percentage return from that close to the latest quote.
            - `latest_price_time_utc`: The timestamp of the latest price.
    """
    returns_df = returns_df.group_by(["market", "exchange"]).agg(
        pl.col("session_return_pct").first().alias("previous_session_return_pct"),
        pl.col("time_utc").first().alias("previous_session_time_utc"),
        pl.col("session_return_pct")
        .shift(-1)
        .first()
        .alias("return_since_previous_close_pct"),
        pl.col("time_utc").shift(-1).first().alias("latest_price_time_utc"),
    )

    return returns_df.sort("market", pl.col("exchange") == "LSE")


def send(data: list[dict]) -> None:
    """Send an email with the market summary.

    Args:
        data: A list of dictionaries containing the market summary data.
    """

    environment = Environment(
        loader=FileSystemLoader(Path(__file__).parents[1] / "templates"),
        autoescape=select_autoescape(["html", "xml"]),
    )

    send_mail(
        to=("axtellpete@gmail.com",),
        subject="Market Summary",
        body=environment.get_template("market_data.html").render(rows=data),
    )


@flow(name="send-market-summary")
def send_market_summary() -> None:
    """Fetch, aggregate, and display the CFD returns report."""

    returns_df = aggregate_by_market_exchange(get_returns_data())

    send(returns_df.to_dicts())


if __name__ == "__main__":
    send_market_summary()
