from . import weather


def test_process_events_empty_input():
    """Test that empty input returns empty output."""
    result = list(weather.process_events([]))
    assert result == []


def test_process_events_passes_through_all_events():
    """Test that all events are yielded back unchanged."""
    events = [
        {"type": "sample", "stationName": "Station1", "temperature": 25.0},
        {"type": "status", "message": "system online"},
        {"type": "sample", "stationName": "Station2", "temperature": 15.0}
    ]
    result = list(weather.process_events(events))
    assert result == events


def test_process_events_handles_invalid_samples():
    """Test that invalid sample events are passed through."""
    events = [
        {"type": "sample", "temperature": 25.0},  # Missing stationName
        {"type": "sample", "stationName": "Station1"},  # Missing temperature
        {"type": "sample", "stationName": "Station2", "temperature": None},  # None temperature
        {"type": "sample", "stationName": "Station3", "temperature": 20.0}  # Valid
    ]
    result = list(weather.process_events(events))
    assert result == events


def test_process_events_handles_zero_and_negative_temps():
    """Test that zero and negative temperatures are processed correctly."""
    events = [
        {"type": "sample", "stationName": "Station1", "temperature": 0.0},
        {"type": "sample", "stationName": "Station1", "temperature": -10.5},
        {"type": "sample", "stationName": "Station1", "temperature": -5.0}
    ]
    result = list(weather.process_events(events))
    assert result == events


def test_process_events_is_generator():
    """Test that the function returns a generator."""
    events = [{"type": "sample", "stationName": "Station1", "temperature": 25.0}]
    result = weather.process_events(events)
    assert hasattr(result, '__iter__')
    assert hasattr(result, '__next__')
    assert list(result) == events
