from functools import cached_property
import requests
from pprint import pprint

class Converter:
    """
    Using Central Bank API
    """

    @cached_property
    def exchange_rates(self):
        r = requests.get('https://www.cbr-xml-daily.ru/daily_json.js')
        return r.json()['Valute']

    def get_valute(self, currency):
        response = self.exchange_rates
        if response.get(currency):
            return response[currency]['Value'] / response[currency]['Nominal']


class ConverterHH:
    """
    Using HH API
    """
    @cached_property
    def exchange_rates(self):
        r = requests.get('https://api.hh.ru/dictionaries/')
        return r.json()['currency']

    def get_valute(self, currency):
        response = self.exchange_rates
        for el in response:
            if currency == el['code']:
                return el['rate']