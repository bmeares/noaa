#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Return the parameters when registering with this plugin.
"""

import meerschaum as mrsm
from meerschaum.utils.typing import Dict, Any

from ._stations import ask_for_stations, get_station_info

def register(pipe: mrsm.Pipe) -> Dict[str, Any]:
    """
    Prompt the user for stations when registering new pipes.
    """
    stations_dict = pipe.parameters.get('noaa', {}).get('stations', {})
    if isinstance(stations_dict, list):
        stations_dict = {stationID: {} for stationID in stations_dict}

    if stations_dict:
        for stationID, station_info in {k: v for k, v in stations_dict.items()}.items():
            if 'name' not in station_info:
                stations_dict[stationID] = get_station_info(stationID)
    else:
        stations_dict = ask_for_stations(pipe)

    return {
        'columns': {
            'datetime': 'timestamp',
            'id': 'station',
        },
        'noaa': {
            'stations': stations_dict,
        },
        'verify': {
            'chunk_minutes': 20160,
        },
    }
