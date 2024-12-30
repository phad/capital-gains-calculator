"""Convert currencies to GBP using rate history."""

from __future__ import annotations

from collections import defaultdict
import csv
import datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Final

from defusedxml import ElementTree as ET
import requests

from .dates import is_date
from .exceptions import ExchangeRateMissingError, ParsingError

if TYPE_CHECKING:
    from .model import BrokerTransaction

EXCHANGE_RATES_HEADER: Final = ["month", "currency", "rate"]
NEW_ENDPOINT_FROM_YEAR: Final = 2021


class CurrencyConverter:
    """Converter which holds rate history."""

    def __init__(
        self,
        exchange_rates_file: str | None = None,
        initial_data: dict[datetime.date, dict[str, Decimal]] | None = None,
    ):
        """Load data from exchange_rates_file and optionally from initial_data."""
        self.exchange_rates_file = exchange_rates_file
        read_data = self._read_exchange_rates_file(exchange_rates_file)
        self.cache: dict[datetime.date, dict[str, Decimal]] = {
            **read_data,
            **(initial_data or {}),
        }
        self.session = requests.Session()

    @staticmethod
    def _read_exchange_rates_file(
        exchange_rates_file: str | None,
    ) -> defaultdict[datetime.date, dict[str, Decimal]]:
        cache: defaultdict[datetime.date, dict[str, Decimal]] = defaultdict(dict)
        if exchange_rates_file is None:
            return cache
        path = Path(exchange_rates_file)
        if not path.is_file():
            return cache
        with path.open(encoding="utf8") as fin:
            csv_reader = csv.DictReader(fin)
            # skip the header
            next(csv_reader)
            for line in csv_reader:
                if sorted(EXCHANGE_RATES_HEADER) != sorted(line.keys()):
                    raise ParsingError(
                        exchange_rates_file,
                        f"invalid columns {line.keys()}, "
                        f"they should be {EXCHANGE_RATES_HEADER}",
                    )
                date = datetime.date.fromisoformat(line["month"])
                cache[date][line["currency"]] = Decimal(line["rate"])
            return cache

    @staticmethod
    def _write_exchange_rates_file(
        exchange_rates_file: str | None, data: dict[datetime.date, dict[str, Decimal]]
    ) -> None:
        if exchange_rates_file is None:
            return
        with Path(exchange_rates_file).open("w", encoding="utf8") as fout:
            data_rows = [
                [month, symbol, str(rate)]
                for month, rates in data.items()
                for symbol, rate in rates.items()
            ]
            writer = csv.writer(fout)
            writer.writerows([EXCHANGE_RATES_HEADER, *data_rows])

    def _query_hmrc_api(self, date: datetime.date) -> None:
        # Pre 2021 we need to use the UK National Archive of the old HMRC endpoint
        # URLs include a datetime (presumably of the time crawled) which we'll have
        # to select per-year. There seem to be no archives of years prior to 2015.
        # Examples:
        # 2015: https://webarchive.nationalarchives.gov.uk/ukgwa/20220504145914mp_/http://www.hmrc.gov.uk/softwaredevelopers/rates/exrates-monthly-1115.XML
        # 2016: https://webarchive.nationalarchives.gov.uk/ukgwa/20220505063703mp_/http://www.hmrc.gov.uk/softwaredevelopers/rates/exrates-monthly-1216.xml
        # 2017: https://webarchive.nationalarchives.gov.uk/ukgwa/20220409150415mp_/http://www.hmrc.gov.uk/softwaredevelopers/rates/exrates-monthly-1217.xml
        # 2018: https://webarchive.nationalarchives.gov.uk/ukgwa/20220409144223mp_/http://www.hmrc.gov.uk/softwaredevelopers/rates/exrates-monthly-1118.xml
        # 2019: https://webarchive.nationalarchives.gov.uk/ukgwa/20220409162528mp_/http://www.hmrc.gov.uk/softwaredevelopers/rates/exrates-monthly-1219.xml
        # 2020: https://webarchive.nationalarchives.gov.uk/ukgwa/20220505131656mp_/http://www.hmrc.gov.uk/softwaredevelopers/rates/exrates-monthly-1220.XML
        crawl_slugs = {
          2015: "20220504145914",
          2016: "20220505063703",
          2017: "20220409150415",
          2018: "20220409144223",
          2019: "20220409162528",
          2020: "20220505131656",
        }
        if date.year < NEW_ENDPOINT_FROM_YEAR:
            month_str = date.strftime("%m%y")
            slug = crawl_slugs[date.year]
            url = (
                f"https://webarchive.nationalarchives.gov.uk/ukgwa/{slug}mp_/"
                "http://www.hmrc.gov.uk/softwaredevelopers/rates/"
                f"exrates-monthly-{month_str}.xml"
            )
        else:
            month_str = date.strftime("%Y-%m")
            url = (
                "https://www.trade-tariff.service.gov.uk/api/v2/"
                f"exchange_rates/files/monthly_xml_{month_str}.xml"
            )
        try:
            response = self.session.get(url, timeout=10)
        except Exception as err:
            msg = f"Error while fetching HMRC exchange rates for the month {month_str} "
            msg += f"from the following url: {url}.\n"
            msg += "Either try again or if you're sure about the rates you can "
            msg += f"add them manually in {self.exchange_rates_file}.\n"
            msg += f"The error was: {err}\n"
            raise ParsingError(url, msg) from err

        if not response.ok:
            raise ParsingError(
                url, f"HMRC API returned a {response.status_code} response"
            )

        tree = ET.fromstring(response.text)
        rates = {
            str(getattr(row.find("currencyCode"), "text", None)).upper(): Decimal(
                str(getattr(row.find("rateNew"), "text", None))
            )
            for row in tree
        }
        if None in rates or None in rates.values():
            raise ParsingError(url, "HMRC API produced invalid/unknown data")
        self.cache[date] = rates
        self._write_exchange_rates_file(self.exchange_rates_file, self.cache)

    def currency_to_gbp_rate(self, currency: str, date: datetime.date) -> Decimal:
        """Get GBP/currency rate at given date."""
        assert is_date(date)
        if date not in self.cache:
            print("Fetching currency conversions for date %s",date)
            self._query_hmrc_api(date)
        if currency not in self.cache[date]:
            raise ExchangeRateMissingError(currency, date)
        return self.cache[date][currency]

    def to_gbp(self, amount: Decimal, currency: str, date: datetime.date) -> Decimal:
        """Convert amount from given currency to GBP."""
        if currency == "GBP":
            return amount
        return amount / self.currency_to_gbp_rate(currency.upper(), date)

    def to_gbp_for(self, amount: Decimal, transaction: BrokerTransaction) -> Decimal:
        """Convert amount from transaction currency to GBP."""

        return self.to_gbp(amount, transaction.currency, transaction.date)
