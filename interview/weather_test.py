import pytest
from . import weather


def test_process_events_empty_input():
    """Test that empty input returns empty output."""
    result = list(weather.process_events([]))
    assert not result

def test_process_events_sample_messages():
    """Test that sample messages are passed through unchanged."""
    events = [
        {"type": "sample", "stationName": "Station1", "temperature": 25.0, "timestamp": 1000},
        {"type": "sample", "stationName": "Station2", "temperature": 15.0, "timestamp": 2000}
    ]
    result = list(weather.process_events(events))
    assert result == events

def test_process_events_snapshot_control():
    """Test snapshot control message generates correct output."""
    events = [
        {"type": "sample", "stationName": "Station1", "temperature": 25.0, "timestamp": 1000},
        {"type": "sample", "stationName": "Station1", "temperature": 30.0, "timestamp": 2000},
        {"type": "sample", "stationName": "Station2", "temperature": 15.0, "timestamp": 3000},
        {"type": "control", "command": "snapshot"}
    ]
    result = list(weather.process_events(events))

    # Should have original sample events, then snapshot output, then control message
    assert len(result) == 5
    assert result[0] == events[0]
    assert result[1] == events[1]
    assert result[2] == events[2]

    # Check snapshot output (generated before control message is yielded)
    snapshot = result[3]
    assert snapshot["type"] == "snapshot"
    assert snapshot["asOf"] == 3000
    assert snapshot["stations"] == {
        "Station1": {"high": 30.0, "low": 25.0},
        "Station2": {"high": 15.0, "low": 15.0}
    }

    # Original control message should be last
    assert result[4] == events[3]


def test_process_events_snapshot_no_data():
    """Test snapshot control message with no sample data is ignored."""
    events = [{"type": "control", "command": "snapshot"}]
    result = list(weather.process_events(events))
    assert result == events


def test_process_events_reset_control():
    """Test reset control message clears data and outputs confirmation."""
    events = [
        {"type": "sample", "stationName": "Station1", "temperature": 25.0, "timestamp": 1000},
        {"type": "control", "command": "reset"},
        {"type": "control", "command": "snapshot"}  # Should be ignored after reset
    ]
    result = list(weather.process_events(events))

    # Should have sample, reset output, control, snapshot (ignored)
    assert len(result) == 4
    assert result[0] == events[0]  # Original sample

    # Check reset output
    reset_output = result[1]
    assert reset_output["type"] == "reset"
    assert reset_output["asOf"] == 1000

    assert result[2] == events[1]  # Original reset control
    assert result[3] == events[2]  # Original snapshot control (no snapshot output after reset)


def test_process_events_reset_no_timestamp():
    """Test reset control message with no prior timestamp."""
    events = [{"type": "control", "command": "reset"}]
    result = list(weather.process_events(events))

    assert len(result) == 2
    reset_output = result[0]
    assert reset_output["type"] == "reset"
    assert reset_output["asOf"] == 0
    assert result[1] == events[0]  # Original control message

def test_process_events_unknown_control_command():
    """Test unknown control command raises exception."""
    events = [{"type": "control", "command": "unknown"}]

    with pytest.raises(ValueError) as exc_info:
        list(weather.process_events(events))

    assert "Unknown control command: unknown" in str(exc_info.value)
    assert "Please verify input." in str(exc_info.value)

def test_process_events_unknown_message_type():
    """Test unknown message type raises exception."""
    events = [{"type": "unknown", "data": "test"}]

    with pytest.raises(ValueError) as exc_info:
        list(weather.process_events(events))

    assert "Unknown message type: unknown" in str(exc_info.value)
    assert "Please verify input." in str(exc_info.value)

def test_process_events_message_without_type():
    """Test message without type field is passed through."""
    events = [{"data": "no type field"}]
    result = list(weather.process_events(events))
    assert result == events


def test_process_events_is_generator():
    """Test that the function returns a generator."""
    events = [{"type": "sample", "stationName": "Station1", "temperature": 25.0}]
    result = weather.process_events(events)
    assert hasattr(result, '__iter__')
    assert hasattr(result, '__next__')
    assert list(result) == events
