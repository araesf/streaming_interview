from typing import Any, Iterable, Generator


def process_events(events: Iterable[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    # Keep track of temperature highs and lows for each station
    stations = {}
    
    # Iterate over each line in the stream
    for line in events:
        if line.get("type") == "sample":
            station_name = line.get("stationName")
            temperature = line.get("temperature")
            
            # Make sure we have a valid station and temperature reading
            if station_name and temperature is not None:
                # Initialize the station's temperature range if we haven't seen it before
                if station_name not in stations:
                    stations[station_name] = {"high": temperature, "low": temperature}
                else:
                    # Update the station's temperature range if the current reading is outside it
                    if temperature > stations[station_name]["high"]:
                        stations[station_name]["high"] = temperature
                    if temperature < stations[station_name]["low"]:
                        stations[station_name]["low"] = temperature
        
        # Yield the line back to the caller
        yield line
