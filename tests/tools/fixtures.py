from uuid import UUID

import pytest
from pandas import Timestamp


@pytest.fixture
def clickstream_data() -> dict:
    data = {
        "event_timestamp": {
            0: Timestamp("2022-07-26 19:50:00.321015"),
            1: Timestamp("2022-07-26 19:50:00.321087"),
            2: Timestamp("2022-07-26 19:50:00.321133"),
            3: Timestamp("2022-07-26 19:50:00.321171"),
            4: Timestamp("2022-07-26 19:50:00.321199"),
            5: Timestamp("2022-07-26 19:50:00.321227"),
            6: Timestamp("2022-07-26 19:50:00.321253"),
            7: Timestamp("2022-07-26 19:50:00.321279"),
            8: Timestamp("2022-07-26 19:50:00.321323"),
            9: Timestamp("2022-07-26 19:50:00.321350"),
            10: Timestamp("2022-07-26 19:50:00.321382"),
            11: Timestamp("2022-07-26 19:50:00.321416"),
            12: Timestamp("2022-07-26 19:50:00.321442"),
            13: Timestamp("2022-07-26 19:50:00.321477"),
            14: Timestamp("2022-07-26 19:50:00.321503"),
        },
        "user_id": {
            0: UUID("6d55bbf7-bf60-42a2-a670-b2deb671fc8b"),
            1: UUID("9c0148e4-2290-4b74-ade4-d84dee5d8fab"),
            2: UUID("8da32442-a7a8-4ab6-ab6a-08b6ca5997ae"),
            3: UUID("f6f15d76-949f-4141-8bd8-97341a73dfd2"),
            4: UUID("a49ef808-214a-4b00-b506-c16b7488ff2f"),
            5: UUID("162a5880-3aab-4acf-8fd1-a10f9892f9ac"),
            6: UUID("066c1477-406f-4af9-91c9-d65cfc1b3b57"),
            7: UUID("2341257f-0e59-4ac4-9b99-f93d3ccb7a7e"),
            8: UUID("cdbbfb43-c53f-4e46-a186-39b21ddf534b"),
            9: UUID("ecde1b62-15be-4b0e-bdf5-2bebee5e9d97"),
            10: UUID("f4a677b8-d155-4a77-a32d-0a0fe1e606c0"),
            11: UUID("5e2a1776-ed8b-49fe-ad1f-b36557b35d22"),
            12: UUID("a61f4748-6cac-4d44-9e84-fd75367faaf7"),
            13: UUID("cb25952c-c80c-4ad0-b5d2-bb785788d84f"),
            14: UUID("a16a840d-c31e-4755-8744-5ea05cc543d5"),
        },
        "screen_width": {
            0: 400,
            1: 1000,
            2: 600,
            3: 400,
            4: 1000,
            5: 400,
            6: 1000,
            7: 1000,
            8: 800,
            9: 1000,
            10: 800,
            11: 1000,
            12: 1000,
            13: 600,
            14: 800,
        },
        "os": {
            0: "iOS 13",
            1: "Android 10",
            2: "win 11",
            3: "Android 7",
            4: "Android 10",
            5: "win 11",
            6: "win 10",
            7: "win 11",
            8: "win 10",
            9: "Android 10",
            10: "mac os x",
            11: "Android 10",
            12: "Android 10",
            13: "win 10",
            14: "win 10",
        },
        "event_name": {
            0: "tracker_created",
            1: "link_click",
            2: "link_click",
            3: "link_click",
            4: "link_click",
            5: "link_click",
            6: "tracker_created",
            7: "tracker_created",
            8: "tracker_created",
            9: "link_click",
            10: "link_click",
            11: "link_click",
            12: "link_click",
            13: "tracker_created",
            14: "tracker_created",
        },
        "device": {
            0: "mobile",
            1: "mobile",
            2: "desktop",
            3: "mobile",
            4: "mobile",
            5: "desktop",
            6: "desktop",
            7: "desktop",
            8: "desktop",
            9: "mobile",
            10: "desktop",
            11: "mobile",
            12: "mobile",
            13: "desktop",
            14: "desktop",
        },
        "utm_source": {
            0: "Insta",
            1: "VK",
            2: "Insta",
            3: "FB",
            4: "Insta",
            5: "VK",
            6: "FB",
            7: "FB",
            8: "FB",
            9: "FB",
            10: "VK",
            11: "Insta",
            12: "Insta",
            13: "VK",
            14: "FB",
        },
        "utm_medium": {
            0: "social",
            1: "social",
            2: "messenger",
            3: "social",
            4: "social",
            5: "messenger",
            6: "messenger",
            7: "messenger",
            8: "social",
            9: "messenger",
            10: "messenger",
            11: "social",
            12: "social",
            13: "messenger",
            14: "messenger",
        },
        "page": {
            0: "special",
            1: "catalog/toys/357907632",
            2: "catalog/toys/710775438",
            3: "special",
            4: "special",
            5: "special",
            6: "special",
            7: "special",
            8: "special",
            9: "catalog/toys/749943719",
            10: "catalog/toys/895066834",
            11: "special",
            12: "catalog/tshirt/607606021",
            13: "special",
            14: "special",
        },
    }
    return data
