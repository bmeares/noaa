#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Example script for syncing NOAA weather data
"""

from __future__ import annotations
from datetime import datetime
from meerschaum.utils.typing import Dict, List, Any, Optional, Iterator
import meerschaum as mrsm
from ._register import register
from ._stations import get_stations, get_station_info, fetch_station_data

__version__ = '1.4.1'
required = ['requests', 'pytz',]

def fetch(
        pipe: mrsm.Pipe,
        begin: Optional[datetime] = None,
        end: Optional[datetime] = None,
        **kw: Any
    ) -> Iterator[Dict[str, List[Any]]]:
    """
    Fetch weather data from `api.weather.gov`.
    """
    stations = get_stations(pipe)
    backtrack_interval = pipe.get_backtrack_interval()
    
    station_sync_times = {
        station: (begin or pipe.get_sync_time(params={'station': station}))
        for station in stations
    }
    station_starts = {
        station: (
            (sync_time - backtrack_interval)
            if sync_time is not None
            else sync_time
        )
        for station, sync_time in station_sync_times.items()
    }

    for station, start in station_starts.items():
        yield fetch_station_data(station, begin=start, end=end)
