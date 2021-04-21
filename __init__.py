#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Example script for syncing NOAA weather data
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Dict, List, Any, Optional

__version__ = '1.2.2'

required = [
    'requests',
]

def get_stations(
        pipe : 'meerschaum.Pipe',
        debug : bool = False
    ) -> dict:
    if 'noaa' not in pipe.parameters:
        pipe.parameters['noaa'] = dict()
    if 'stations' not in pipe.parameters['noaa']:
        pipe.parameters['noaa']['stations'] = dict()
    if pipe.parameters['noaa']['stations'] is None:
        pipe.parameters['noaa']['stations'] = dict()

    ### Return if we've already fetched stations.
    if len(pipe.parameters['noaa']['stations']) > 0:
        return pipe.parameters['noaa']['stations']

    import requests, json, re
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.prompt import yes_no, prompt
    from meerschaum.utils.formatting import pprint

    instructions = f"""
    Visit https://www.weather.gov and use the local forecast search tool
    on the top left to find specific station IDs (e.g. 'KATL' for Atanta).

    To fetch all stations from a state, enter the state abbreviation
    (e.g. 'GA' for Georgia).

    NOTE: This will be slow! In the future, run with --async to sync more quickly.
    """
    info(instructions)

    stations = dict()

    while True:
        stationID = prompt("Enter station ID or state abbreviation, empty to stop: ", icon=False)
        if stationID == '': break
        if len(stationID) == 2:
            state_abbrev = stationID
            if yes_no(
                f"Are you sure you want to fetch from all stations in the state '{state_abbrev}'? " +
                "This will be very slow!"
            ):
                stations = get_state_stations(state_abbrev)
                break

        url = f"https://api.weather.gov/stations/{stationID}"
        info = json.loads(requests.get(url).text)
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

        stations[stationID] = dict()
        stations[stationID]['name'] = name
        if geo is not None: stations[stationID]['geometry'] = geo

    pprint(stations)
    if not yes_no(f"Would you like to register the above stations to pipe '{pipe}'?"):
        print("Resetting stations and starting over...")
        pipe.parameters['noaa']['stations'] = dict()
        return get_stations(pipe, debug=debug)

    pipe.parameters['noaa']['stations'] = stations
    pipe.edit(interactive=False, debug=debug)
    return stations

def get_state_stations(
        state_abbrev : str,
        debug : bool = False
    ) -> dict:
    """
    Parse every station in a state
    """
    from meerschaum.utils.warnings import warn
    import requests, json
    url = f"https://api.weather.gov/stations"
    stations = dict()
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

def sync(
        pipe : 'meerschaum.Pipe',
        debug : bool = False,
        blocking : bool = True,
        workers : Optional[int] = None,
        **kw
    ) -> SuccessTuple:
    """
    Fetch JSON data from NOAA and sync it into a Pipe.
    Overrides the default Meerschaum sync function.
    """
    from multiprocessing.pool import ThreadPool
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, info

    ### Specify the columns in case Pipe is not registered.
    ### NOTE: Normally the Pipe's columns' types are determined by the first dataframe encountered.
    ###       In this script, we cast everything to floats to avoid integers.
    if not pipe.columns:
        pipe.columns = {
            "datetime" : "timestamp",
            "id" : "station",
        }
        pipe.edit(interactive=False, debug=debug)

    ### dictionary of NOAA weather stations and names
    stations = get_stations(pipe, debug=debug)
    if workers is None:
        workers = int(len(stations) / 2) + 1

    ### Fetch data from the stations.
    try:
        pool = ThreadPool(workers)
    except Exception as e:
        print(e)
        pool = None
    args = [(stationID, info, pipe) for stationID, info in stations.items()]
    dataframes = (
        dict(pool.starmap(do_fetch, args)) if pool is not None
        else dict([do_fetch(*a) for a in args])
    )
    if pool is not None:
        pool.close()
        pool.join()

    ### only keep the common columns (skipping empty dataframes)
    common_cols = None
    for stationID, df in dataframes.items():
        if df is None: continue
        #  print(df)
        if len(df.columns) == 0: continue
        #  df.rename(columns=(lambda x : x.lstrip().rstrip()), inplace=True)
        if common_cols is None:
            common_cols = list(set(df.columns))
            continue
        try:
            common_cols = list(set(common_cols) & set(df.columns))
        except Exception as e:
            warn(str(e))
    ### Make empty set in case all dataframes are empty.
    if common_cols is None: common_cols = list()
    ### Pandas needs the columns to be in the same order, so sort the columns.
    common_cols.sort()

    ### Cast all but these columns to floats.
    non_float_cols = sorted(list({'label', 'timestamp', 'station', 'location', 'geometry'}))
    float_cols = sorted(list(set(common_cols) - set(non_float_cols)))

    ### Cast the value columns to floats to avoid integers.
    _dataframes = dict()
    for stationID, df in dataframes.items():
        if df is not None:
            try:
                ### Only keep commons columns and ensure they are sorted.
                if debug:
                    dprint(f"Common columns: {common_cols}")
                df = df[common_cols]
                df[float_cols] = df[float_cols].astype('float')
            except Exception as e:
                if debug:
                    warn(str(e))
                warn(
                    f"Unable to parse data from station '{stationID}' " +
                    f"({stations[stationID]['name']})",
                    stack = False
                )
                df = None
            _dataframes[stationID] = df
    dataframes = _dataframes

    ### Make sure Pipe exists.
    ### Normally this is handled when syncing for the first time, but threading breaks things.
    if not pipe.exists(debug=debug):
        for stationID, df in dataframes.items():
            if df is not None:
                if len(df) > 0:
                    pipe.sync(df.head(1), force=True, debug=debug)
                    break

    ### Finally, time to sync the dataframes.
    ### pipe.sync returns a tuple of success bool and message.
    ### E.g. (True, "Success") or (False, "Error message")
    success_dict = dict()
    for stationID, df in dataframes.items():
        info(f"Syncing data from station '{stationID}' ({stations[stationID]['name']})...")
        kw.update({
            'blocking' : blocking, 'workers' : workers, 'debug' : debug,
        })
        success = pipe.sync(df, **kw)[0] if df is not None else False
        success_dict[stationID] = success

    succeeded, failed = 0, 0
    for stationID, success in success_dict.items():
        if not success:
            warn(
                f"Failed to sync from station '{stationID}' ({stations[stationID]['name']})",
                stack = False
            )
            failed += 1
        else:
            succeeded += 1

    return (succeeded > 0), f"Synced from {succeeded + failed} stations, {failed} failed."

def do_fetch(
        stationID : str,
        info : dict,
        pipe : 'meerschaum.Pipe'
    ) -> Tuple[str, Optional[Dict[str, List[Any]]]]:
    """
    Wrapper for fetch_station_data (below)
    """
    from meerschaum.utils.warnings import warn
    try:
        df = fetch_station_data(stationID, info, pipe)
    except Exception as e:
        msg = str(e)
        warn(f"Failed to sync station '{stationID}' ({info['name']}). Error:\n{msg}")
        df = None

    return stationID, df

def fetch_station_data(
        stationID : str,
        info : dict,
        pipe : meerschaum.Pipe
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
    try:
        start = (
            pipe.get_sync_time(
                { "station" : stationID }
            ) - datetime.timedelta(hours=24)
        ).replace(
            tzinfo = pytz.timezone('UTC')
        ).isoformat()
    except Exception as e:
        start = None

    ### fetch JSON from NOAA since the start time (sync_time for this stationID)
    if start:
        print(
            f"Fetching data newer than {start} for station '{stationID}' ({info['name']})...",
            flush = True
        )
    else:
        print(
            f"Fetching all possible data for station '{stationID}' ({info['name']})...",
            flush = True
        )
        
    url = f"https://api.weather.gov/stations/{stationID}/observations/"
    response = None
    try:
        response = requests.get(url, params={"start":start})
        data = json.loads(response.text)
    except Exception as e:
        print(f"\nFailed to parse JSON with exception: {e}", flush=True)
        if response is not None:
            print("Received text:\n" + response.text)
        return None
    print(f"Done fetching data for station '{stationID}' ({info['name']}).", flush=True)

    ### build a dictionary from the JSON response (flattens JSON)
    d = dict()
    if 'features' not in data:
        warn(
            f"Failed to fetch data for station '{stationID}' ({info['name']}):\n" + str(data),
            stack = False
        )
        return None

    for record in data['features']:
        properties = record['properties']

        if 'location' not in d:
            d['location'] = []
        d['location'].append(info['name'])

        if 'geometry' not in d:
            d['geometry'] = []
        geo = None
        if 'geometry' in info:
            geo = json.dumps(info['geometry'])
        d['geometry'].append(geo)

        for col, v in properties.items():
            ### Specific to this API; filter out features we don't want.
            if not v:
                continue
            ### At this point, the timestamp is a string.
            ### It will get casted below in `parse_df_datetimes`.
            if col == 'timestamp':
                val = v
            ### We could just use the stationID provided, but it's given in the JSON
            ### so we might as well use it.
            elif col == 'station': val = v.split('/')[-1]

            ### Skip features that don't contain a simple 'value' key.
            ### NOTE: this will need to be tweaked if we want more information.
            elif not isinstance(v, dict):
                continue
            elif 'value' not in v:
                continue
            else:
                val = v['value']

            ### If possible, append units to column name.
            if isinstance(v, dict):
                if 'unitCode' in v:
                    col += " (" + v['unitCode'].replace('unit:', '') + ")"

            ### Grow the lists in the dictionary.
            ### E.g. { 'col1' : [ 1, 2, 3 ], 'col2' : [ 4, 5, 6 ] }
            if col not in d:
                d[col] = []
            d[col].append(val)

    ### Normalize the lengths.
    klens, lens = dict(), dict()
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

    ### Create a pandas DataFrame from the dictionary and parse for datetimes.
    return parse_df_datetimes(pd.DataFrame(norm_d))
