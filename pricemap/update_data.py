import json
import logging
import re
from datetime import datetime

from flask import g

import psycopg2
import psycopg2.extras

from parse import parse

import requests


LISTINGS_API_RESPONSE_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "listing_id": {
                "type": "string",
            },
            "title": {
                "type": "string",
            },
            "price": {
                "type": "string",
            },
        },
        "required": ["listing_id", "title", "price"],
    },
}


def get_palaces_ids():
    query = "select id from geo_place"
    cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query)
    rows = cursor.fetchall()
    geoms_ids = [row[0] for row in rows]
    g.db.commit()
    return geoms_ids


def init_database():
    sql = """
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER NOT NULL,
            place_id INTEGER  NOT NULL,
            price INTEGER  NOT NULL,
            area INTEGER  NOT NULL,
            room_count INTEGER,
            first_seen_at TIMESTAMP  NOT NULL,
            last_seen_at TIMESTAMP  NOT NULL,
            PRIMARY KEY (id)
        );
    """
    cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cursor.execute(sql)
        g.db.commit()
    except:
        g.db.rollback()
        print("Error: maybe table already exists?")
        return


def extract_listing(item):
    listing_id = item["listing_id"]

    # parse title
    title_formats = [
        "Appartement{room_count:d}pièces-{area:d}m²",
        "Studio-{area:d}m²",
        "Appartement-{area:d}m²",
    ]

    parse_title_results = (
        parse(title_format, re.sub(r"\s", "", item["title"]))
        for title_format in title_formats
    )

    title_fields = next(
        (
            parse_title_result.named
            for parse_title_result in parse_title_results
            if parse_title_result
        ),
        {},
    )

    # extract room count
    room_count = title_fields.get("room_count")

    # extract area
    if "area" not in title_fields:
        logging.error("failed to extract area")
        # raise exception because area is required
        raise ValueError("area not found")
    area = title_fields["area"]

    # extratc price
    price_format = "{price:d}€"
    # remove the special Narrow No-Break Space
    parse_price_result = parse(price_format, re.sub(r"\s", "", item["price"]))
    if not parse_price_result:
        logging.error("failed to extract price")
        # raise exception because price is required
        raise ValueError("price not found")
    price = parse_price_result.named["price"]

    return {
        "listing_id": listing_id,
        "room_count": room_count,
        "price": price,
        "area": area,
    }


def update():
    update_time = datetime.now()

    logging.error(f"update listings: {update_time}")
    # init database
    init_database()
    places_ids = get_palaces_ids()
    logging.error(f"places to collect: {places_ids}")

    listings = []
    for place_id in places_ids:
        logging.error(f"read place : {place_id}")
        page = 0

        items_nbr_per_place = 0
        while True:
            page += 1
            logging.error(f"read page : {page}")
            url = f"http://listingapi:5000/listings/{place_id}?page={page}"
            response = requests.get(url)

            if response.status_code == 416:
                logging.error("no more page retrieve next place")
                logging.error(f"items_nbr_per_place {place_id} : {items_nbr_per_place}")
                break
            elif response.status_code != 200:

                logging.error(
                    f" listing api failed, http status : {response.status_code}"
                )
                break

            # TODO validate reponse schema here
            items = response.json()
            logging.error(f"number of items: {len(items)}")
            items_nbr_per_place += len(items)
            for item in items:
                try:
                    listing = extract_listing(item)
                except Exception as err:
                    logging.error(
                        f"invalid listing from api: {json.dumps(item)}, error: {err}"
                    )
                    # ignore invalid listing it and continue
                    continue

                listing["place_id"] = place_id
                listing["first_seen_at"] = update_time
                listing["last_seen_at"] = update_time
                listings.append(listing)

    if listings:
        logging.error("update database")
        query = """
                    INSERT INTO listings VALUES(
                        %(listing_id)s,
                        %(place_id)s,
                        %(price)s,
                        %(area)s,
                        %(room_count)s,
                        %(first_seen_at)s,
                        %(last_seen_at)s
                    )
                    ON CONFLICT (id) DO UPDATE
                    SET last_seen_at = %(last_seen_at)s
            """

        db_cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extras.execute_batch(db_cursor, query, listings, page_size=100)
        g.db.commit()
        logging.error(f"listings updated successfully")


GEOMS_IDS = [
    32684,
    32683,
    32682,
    32685,
    32686,
    32687,
    32688,
    32689,
    32690,
    32691,
    32692,
    32693,
    32699,
    32694,
    32695,
    32696,
    32697,
    32698,
    32700,
    32701,
]


def update_old():
    # init database
    init_database()

    db_cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    for geom in GEOMS_IDS:
        p = 0
        while True:
            p += 1
            url = "http://listingapi:5000/listings/" + str(geom) + "?page=" + str(p)
            d = requests.get(url)

            # Break when finished
            if d.status_code == 416:
                break

            for item in d.json():
                listing_id = item["listing_id"]
                try:
                    room_count = (
                        1
                        if "Studio" in item["title"]
                        else int(
                            "".join(
                                [
                                    s
                                    for s in item["title"].split("pièces")[0]
                                    if s.isdigit()
                                ]
                            )
                        )
                    )
                except:
                    room_count = 0

                try:
                    price = int("".join([s for s in item["price"] if s.isdigit()]))
                except:
                    price = 0

                try:
                    area = int(
                        item["title"]
                        .split("-")[1]
                        .replace(" ", "")
                        .replace("\u00a0m\u00b2", "")
                    )
                except:
                    area = 0

                seen_at = datetime.now()

                now = datetime.now()

                sql = f"""
                    INSERT INTO listings VALUES(
                        {listing_id},
                        {geom},
                        {price},
                        {area},
                        {room_count},
                        '{seen_at}'
                    );
                """
                db_cursor.execute(sql)
                g.db.commit()
