from typing import Any, Iterable, Generator

# Named constants
DEFAULT_TIMESTAMP_ON_RESET = 0


def _is_valid_sample_data(station_name: str | None, temperature: float | None) -> bool:
    """Encapsulate the boundary condition for valid sample data."""
    has_station_name = bool(station_name)
    has_temperature = temperature is not None
    return has_station_name and has_temperature


def _process_sample_event(
    line: dict[str, Any], stations: dict, most_recent_timestamp: int | None
) -> int | None:
    """Process a sample event and update station data."""
    station_name = line.get("stationName")
    temperature = line.get("temperature")
    timestamp = line.get("timestamp")

    # Update most recent timestamp using explanatory variable
    has_timestamp = timestamp is not None
    if has_timestamp:
        most_recent_timestamp = timestamp

    # Use encapsulated boundary condition
    if _is_valid_sample_data(station_name, temperature):
        assert station_name is not None
        assert temperature is not None
        _update_station_temperature(stations, station_name, temperature)
    return most_recent_timestamp

def _update_station_temperature(stations: dict, station_name: str, temperature: float) -> None:
    """Update temperature range for a station."""
    if station_name not in stations:
        stations[station_name] = {"high": temperature, "low": temperature}
    else:
        # Update the station's temperature range if the current reading is outside it
        if temperature > stations[station_name]["high"]:
            stations[station_name]["high"] = temperature
        if temperature < stations[station_name]["low"]:
            stations[station_name]["low"] = temperature

def _process_control_event(
    line: dict[str, Any], stations: dict, most_recent_timestamp: int | None
) -> Generator[dict[str, Any], None, None]:
    """Process a control event and yield any output."""
    command = line.get("command")

    if command == "snapshot":
        yield from _handle_snapshot_command(stations, most_recent_timestamp)
    elif command == "reset":
        yield from _handle_reset_command(stations, most_recent_timestamp)
    else:
        raise ValueError(f"Unknown control command: {command}")

def _handle_snapshot_command(
    stations: dict, most_recent_timestamp: int | None
) -> Generator[dict[str, Any], None, None]:
    """Handle snapshot command."""
    has_station_data = bool(stations)
    has_timestamp = most_recent_timestamp is not None

    if has_station_data and has_timestamp:
        snapshot_output = {
            "type": "snapshot",
            "asOf": most_recent_timestamp,
            "stations": dict(stations)
        }
        yield snapshot_output

def _handle_reset_command(
    stations: dict, most_recent_timestamp: int | None
) -> Generator[dict[str, Any], None, None]:
    """Handle reset command."""
    timestamp_for_reset = (
        most_recent_timestamp if most_recent_timestamp else DEFAULT_TIMESTAMP_ON_RESET
    )
    reset_output = {
        "type": "reset",
        "asOf": timestamp_for_reset
    }
    stations.clear()
    yield reset_output

def process_events(events: Iterable[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    """Process a stream of weather events."""
    # Keep track of temperature highs and lows for each station
    stations: dict[str, dict[str, float]] = {}
    # Track the most recent timestamp for asOf field
    most_recent_timestamp = None

    # Iterate over each line in the stream
    for line in events:
        message_type = line.get("type")
        if message_type == "sample":
            most_recent_timestamp = _process_sample_event(line, stations, most_recent_timestamp)
        elif message_type == "control":
            yield from _process_control_event(line, stations, most_recent_timestamp)
        elif message_type:
            raise ValueError(f"Unknown message type: {message_type}")
        else:
            yield line
            continue

        yield line
