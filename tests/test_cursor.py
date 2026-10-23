import pytest

from bluesky_stream_analytics.cursor import load_cursor, save_cursor


def test_missing_cursor_means_start_live(tmp_path):
    assert load_cursor(tmp_path / "cursor.json") is None


def test_save_then_load_round_trips(tmp_path):
    path = tmp_path / "state" / "cursor.json"
    save_cursor(path, 1761224400123456)
    assert load_cursor(path) == 1761224400123456


def test_save_overwrites_and_leaves_no_temp_file(tmp_path):
    path = tmp_path / "cursor.json"
    save_cursor(path, 1)
    save_cursor(path, 2)
    assert load_cursor(path) == 2
    assert [p.name for p in tmp_path.iterdir()] == ["cursor.json"]


@pytest.mark.parametrize("content", ["", "not json", "{}", '{"time_us": "abc"}'])
def test_unreadable_cursor_raises_instead_of_starting_live(tmp_path, content):
    path = tmp_path / "cursor.json"
    path.write_text(content)
    with pytest.raises(ValueError):
        load_cursor(path)
