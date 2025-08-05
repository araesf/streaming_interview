from typing import Any, Iterable, Generator


def _process_sample_event(
    line: dict[str, Any], stations: dict, most_recent_timestamp: int | None
) -> int | None:
    """Process a sample event and update station data."""
    station_name = line.get("stationName")
    temperature = line.get("temperature")
    timestamp = line.get("timestamp")

    # Update most recent timestamp
    if timestamp is not None:
        most_recent_timestamp = timestamp

    # Make sure we have a valid station and temperature reading
    if station_name and temperature is not None:
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
        raise ValueError(f"Unknown control command: {command}. Please verify input.")


def _handle_snapshot_command(
    stations: dict, most_recent_timestamp: int | None
) -> Generator[dict[str, Any], None, None]:
    """Handle snapshot command."""
    # Only output snapshot if we have sample data
    if stations and most_recent_timestamp is not None:
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
    # Output reset confirmation with current timestamp
    reset_output = {
        "type": "reset",
        "asOf": most_recent_timestamp if most_recent_timestamp is not None else 0
    }
    # Clear all station data
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
        elif message_type is not None:
            # Unknown message type
            raise ValueError(f"Unknown message type: {message_type}. Please verify input.")
        else:
            # Message without type field - yield as-is
            yield line
            continue

        # For sample and control messages, yield the original line
        yield line
