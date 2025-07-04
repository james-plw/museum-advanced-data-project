import pytest
from loading import check_msg_validity

test_data = [
    ({'at': '2025-07-03T11:54:02.089583+01:00', 'site': '0', 'val': 1},
     [None, None, None]),
    ({'at': '2025-07-03T11:55:19.098464+01:00', 'site': '3', 'val': 3},
     [None, None, None]),
    ({'at': '2025-07-03T11:56:31.105133+01:00', 'site': '2', 'val': 1},
     [None, None, None]),
    ({'at': '2025-07-03T12:15:51.221499+01:00', 'site': '2', 'val': -1, 'type': 0},
     [None, None, None, None]),

    ({},
     ["INVALID: missing 'at' key", "INVALID: missing 'site' key", "INVALID: missing 'val' key"]),

    ({'at': '2025-07-01T15:19:23.897509+01:00', 'site': '4', 'val': -3},
     [None, None, "INVALID: 'val' is out of range"]),
    ({'at': '2025-07-01T15:19:38.899136+01:00', 'site': '3', 'val': 'INF'},
     [None, None, "INVALID: 'val' must be an integer"]),

    ({'at': '2025-07-01T15:20:02.901590+01:00', 'site': '11', 'val': 4},
     [None, "INVALID: 'site' is out of range", None]),
    ({'at': '2025-07-01T15:24:08.926205+01:00', 'site': 'None', 'val': 4},
     [None, "INVALID: 'site' must be an integer", None]),

    ({'at': '2025-07-01T15:26:53.942743+01:00', 'site': '3', 'val': -1},
     [None, None, None, "INVALID: missing 'type' key"]),
    ({'at': '2025-07-01T15:26:53.942743+01:00', 'site': '3', 'val': -1, 'type': 2},
     [None, None, None, "INVALID: 'type' is out of range"]),
    ({'at': '2025-07-01T15:27:39.947220+01:00', 'site': '1', 'val': -1, 'type': 'ERR'},
     [None, None, None, "INVALID: 'type' must be an integer"]),

    ({'site': '4', 'val': 1}, ["INVALID: missing 'at' key", None, None]),
    ({'at': '2025-07-03T00:01:22.055608+01:00', 'site': '3', 'val': 4},
     ["INVALID: 'at' is outside the valid time range", None, None])
]


@pytest.mark.parametrize('value,result', test_data)
def test_check_data(value, result):
    validity_checks = [check_msg_validity(value, 'at'),
                       check_msg_validity(value, 'site'),
                       check_msg_validity(value, 'val')]
    if value.get('val') == -1:
        validity_checks.append(check_msg_validity(value, 'type'))
    assert validity_checks == result
