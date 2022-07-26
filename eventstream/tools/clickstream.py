import datetime
import uuid
from random import choice, randint
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pandas._typing import DataFrame


def _generate_page() -> str:
    if randint(0, 10) % 4:
        return "special"
    else:
        return "/".join(["catalog", choice(["toys", "shoes", "tshirt"]), str(randint(10**8, 9 * (10**8)))])


def _generate_clickstream_row() -> dict:
    awailable_os = {
        "desktop": ["mac os x", "win 10", "win 11"],
        "mobile": ["iOS 13", "iOS 14", "Android 7", "Android 10"],
    }

    device = choice(["desktop", "mobile"])
    row = {
        "event_timestamp": datetime.datetime.now(),
        "user_id": uuid.uuid4(),
        "screen_width": choice([400, 600, 800, 1000]),
        "os": choice(awailable_os[device]),
        "event_name": choice(["tracker_created", "link_click"]),
        "device": device,
        "utm_source": choice(["VK", "FB", "Insta"]),
        "utm_medium": choice(["social", "messenger"]),
        "page": _generate_page(),
    }
    return row


def generate_clickstream(rows: int = 50) -> "DataFrame":
    sample_data = [_generate_clickstream_row() for _ in range(rows)]
    df = pd.DataFrame(
        data=sample_data,
    )

    return df
