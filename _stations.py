#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Station utility functions.
"""

import meerschaum as mrsm
from meerschaum.utils.typing import Dict, List, Any, Optional
STATIONS_BASE_URL: str = "https://api.weather.gov/stations"

_stations_info_cache: Dict[str, Any] = {}

def get_station_info(stationID: str) -> Dict[str, Any]:
    """
    Fetch the metadata for a station.
    """
    from meerschaum.utils.warnings import warn
    import requests
    station_info = _stations_info_cache.get(stationID, {})
    if station_info:
        return station_info
    url = STATIONS_BASE_URL + "/" + stationID
    response = requests.get(url)
    if not response:
        warn(
            f"Unable to get information for station '{stationID}':\n{response.text}",
            stack = False,
        )
        return station_info

    info = response.json()

    try:
        geo = info['geometry']
    except Exception as e:
        geo = None
    try:
        name = info['properties']['name'].rstrip()
    except Exception as e:
        warn(f"Unable to fetch the name for station '{stationID}'.", stack=False)
        return station_info

    station_info['name'] = name
    if geo is not None:
        station_info['geometry'] = geo
    _stations_info_cache[stationID] = station_info
    return station_info


def ask_for_stations(pipe, debug: bool = False) -> Dict[str, Any]:
    """
    Prompt the user for stations and return a dictionary.
    """
    import requests, json, re
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.prompt import yes_no, prompt
    from meerschaum.utils.formatting import pprint

    instructions = f"""
    Visit https://www.weather.gov and use the local forecast search tool
    on the top left to find specific station IDs (e.g. 'KATL' for Atanta).

    To fetch all stations from a state, enter the state abbreviation
    (e.g. 'GA' for Georgia).
    """
    info(instructions)

    stations = {}

    while True:
        stationID = prompt("Enter station ID or state abbreviation, empty to stop: ", icon=False)
        if stationID == '':
            break

        if len(stationID) == 2:
            state_abbrev = stationID
            if yes_no(
                f"Are you sure you want to fetch from all stations in the state '{state_abbrev}'? " +
                "This will be very slow!"
            ):
                stations = get_state_stations(state_abbrev)
                break

        url = STATIONS_BASE_URL + "/" + stationID
        response = requests.get(url)
        if not response:
            warn(
                f"Unable to get information for station '{stationID}':\n{response.text}",
                stack = False,
            )
            continue

        info = response.json()

        try:
            geo = info['geometry']
        except:
            geo = None
        try:
            name = info['properties']['name'].rstrip()
        except:
            warn(f"Unable to fetch name for station '{stationID}'. Skipping...", stack=False)
            continue

        if not yes_no(f"Is '{name}' a good label for station '{stationID}'?"):
            name = prompt(f"New label for station '{stationID}': ", icon=False)

        stations[stationID] = {}
        stations[stationID]['name'] = name
        if geo is not None:
            stations[stationID]['geometry'] = geo

    pprint(stations)
    if not yes_no(f"Would you like to register the above stations to pipe '{pipe}'?"):
        print("Resetting stations and starting over...")
        pipe.parameters['noaa']['stations'] = {}
        return ask_for_stations(pipe, debug=debug)

    return stations


def get_stations(pipe: 'mrsm.Pipe') -> Dict[str, Any]:
    """
    Return the stations dictionary.
    """
    edit = False
    stations_dict = pipe.parameters.get('noaa', {}).get('stations', {})
    if isinstance(stations_dict, list):
        stations_dict = {stationID: {} for stationID in stations_dict}

    for stationID, station_info in {k: v for k, v in stations_dict.items()}.items():
        if 'name' not in station_info:
            stations_dict[stationID] = get_station_info(stationID)
            edit = True

    if edit:
        pipe.parameters['noaa']['stations'] = stations_dict
        pipe.edit()

    try:
        return pipe.parameters['noaa']['stations']
    except Exception as e:
        return {}


def get_state_stations(
        state_abbrev: str,
        debug: bool = False
    ) -> dict:
    """
    Parse every station in a state
    """
    from meerschaum.utils.warnings import warn
    import requests, json
    url = "https://api.weather.gov/stations"
    stations = {}
    print(f"Retrieving stations for state '{state_abbrev}'...")
    d = json.loads(requests.get(url, params={'state' : state_abbrev}).text)
    if 'features' not in d:
        warn(f"No stations retrieved for state '{state_abbrev}'.", stack=False)
        return stations
    for f in d['features']:
        stationID = None
        try:
            stationID = f['id'].split('/stations/')[-1]
            geo = f.get('geometry', None)
            name = f['properties']['name'].lstrip().rstrip()
        except:
            if stationID is not None:
                warn(f"Could not determine name for station '{stationID}'. Skipping...")
            continue
        stations[stationID] = dict()
        stations[stationID]['name'] = name
        stations[stationID]['geometry'] = geo
    return stations


def fetch_station_data(
        stationID: str,
        begin: Optional['datetime.datetime'] = None,
        end: Optional['datetime.datetime'] = None,
    ) -> Optional[Dict[str, List[Any]]]:
    """
    Fetch JSON for a given stationID from NOAA and parse into a dataframe
    """
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.misc import parse_df_datetimes
    from meerschaum.utils.warnings import warn
    import json, pytz, datetime, requests
    pd = import_pandas()
    ### Get the latest sync time for this station so we don't request duplicate data.
    start = (
        begin.replace(tzinfo=pytz.timezone('UTC')).isoformat()
        if begin is not None
        else None
    )
    end = (
        end.replace(tzinfo=pytz.timezone('UTC')).isoformat()
        if end is not None
        else None
    )
    info_dict = get_station_info(stationID)

    print(
        (f"{start} - {end}\n" if start else '')
        + f"Fetching data for station '{stationID}' ({info_dict['name']})..."
    )
        
    url = f"https://api.weather.gov/stations/{stationID}/observations/"
    response = None
    try:
        response = requests.get(url, params={"start":start, "end": end})
        data = json.loads(response.text)
    except Exception as e:
        print(f"\nFailed to parse JSON with exception: {e}", flush=True)
        if response is not None:
            print("Received text:\n" + response.text)
        return None
    print(f"Done fetching data for station '{stationID}' ({info_dict['name']}).", flush=True)

    ### build a dictionary from the JSON response.
    d = {'location': [], 'geometry': [], 'cloudLayers': [] }
    if 'features' not in data:
        warn(
            f"Failed to fetch data for station '{stationID}' ({info_dict['name']}):\n" + str(data),
            stack = False
        )
        return None

    for record in data['features']:
        d['location'].append(info_dict.get('name', None))
        d['geometry'].append(info_dict.get('geometry', {}))

        for col, v in record.get('properties', {}).items():
            if col.startswith('@'):
                continue

            if col == 'timestamp':
                val = v
            ### We could just use the stationID provided, but it's given in the JSON
            ### so we might as well use it.
            elif col == 'station':
                val = v.split('/')[-1]
            elif isinstance(v, dict) and 'value' in v:
                val = v['value']
            else:
                val = v

            ### If possible, append units to column name.
            if isinstance(v, dict) and 'unitCode' in v:
                col += " (" + v['unitCode'].replace('wmoUnit:', '') + ")"

            if col == 'cloudLayers' and val is None:
                val = []

            ### Grow the lists in the dictionary.
            ### E.g. { 'col1' : [ 1, 2, 3 ], 'col2' : [ 4, 5, 6 ] }
            if col not in d:
                d[col] = []
            d[col].append(val)

    ### Normalize the lengths.
    klens, lens = {}, {}
    for k, v in d.items():
        klens[k] = len(v)
    for k, l in klens.items():
        if l not in lens:
            lens[l] = 0
        lens[l] += 1
    max_l, max_c = 0, 0
    for l, c in lens.items():
        if c > max_c:
            max_c = c
            max_l = l
    norm_keys = [k for k, l in klens.items() if l == max_l]
    norm_d = {k: d[k] for k in norm_keys}
    return norm_d
