#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch county-level COVID data from the state of Colorado.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any

BASE_URL = 'https://services3.arcgis.com/66aUo8zsujfVXRIT/arcgis/rest/services/colorado_covid19_county_statistics_cumulative/FeatureServer/0/query'
required = ['requests', 'python-dateutil']

def register(pipe: meerschaum.Pipe, **kw):
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.prompt import prompt, yes_no
    while True:
        fips_str = prompt("Please enter a list of FIPS codes separated by commas:")
        fips = fips_str.replace(' ', '').split(',')

        valid = True
        for f in fips:
            if not f.startswith("08"):
                warn("All FIPS codes must begin with 08 (prefix for the state of Colorado).", stack=False)
                valid = False
                break
        if not valid:
            continue

        question = "Is this correct?"
        for f in fips:
            question += f"\n  - {f}"
        question += '\n'

        if not fips or not yes_no(question):
            continue
        break

    return {
        'columns': {
            'datetime': 'date',
            'id': 'fips',
            'value': 'cases'
        },
        'CO-covid': {
            'fips': fips,
        },
    }

def fetch(
        pipe: meerschaum.Pipe,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None, 
        debug: bool = False,
        **kw
    ) -> Dict[str, Any]:
    from dateutil import parser
    import datetime, requests, pandas as pd
    from meerschaum.utils.formatting import pprint

    fips = pipe.parameters['CO-covid']['fips']
    fips_where = "'" + "', '".join([f[2:] for f in fips]) + "'"
    st = pipe.get_sync_time(debug=debug)
    where = f"FIPS IN ({fips_where}) AND Metric IN ('Cases', 'Deaths')"
    begin = begin if begin is not None else pipe.get_sync_time(debug=debug)
    if begin is not None:
        where += f" AND CAST(Date AS DATE) >= CAST(\'{begin.strftime('%m/%d/%Y')}\' AS DATE)"
    if end is not None:
        where += f" AND CAST(Date AS DATE) <= CAST(\'{end.strftime('%m/%d/%Y')}\' AS DATE)"
    params = {
        'where': where,
        'outFields': 'COUNTY,FIPS,Metric,Value,Date',
        'f': 'json',
    }
    if debug:
        pprint(params)

    dtypes = {
        'date': 'datetime64[ms]',
        'county': str,
        'fips': str,
        'cases': int,
        'deaths': int,
    }

    final_data = {
        'date': [],
        'county': [],
        'fips': [],
        'cases': [],
        'deaths': [],
    }

    data = requests.get(BASE_URL, params=params).json()
    if debug:
        pprint(data)
    for i, row in enumerate(data['features']):
        attrs = row['attributes']
        if attrs['Metric'] == 'Cases':
            continue
        final_data['date'].append(parser.parse(attrs['Date']))
        final_data['county'].append(attrs['COUNTY'].lower().capitalize())
        final_data['fips'].append('08' + attrs['FIPS'])
        final_data['deaths'].append(int(attrs['Value']))
        final_data['cases'].append(int(data['features'][i + 1]['attributes']['Value']))
    if debug:
        pprint(final_data)
    return final_data
