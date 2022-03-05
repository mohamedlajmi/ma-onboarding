import json
import logging
import re
from datetime import datetime
from itertools import count

from flask import g

# from jsonschema import validate, ValidationError

from parse import parse

import psycopg2
import psycopg2.extras

import requests


LISTINGS_API_URL = "http://listingapi:5000/listings"
LISTINGS_API_PAGE_SIZE = 20
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
    g.db_cursor.execute(query)
    geo_places = g.db_cursor.fetchall()
    logging.error(f"geo_places: {geo_places}")
    geoms_ids = [geo_place["id"] for geo_place in geo_places]
    return geoms_ids


def init_database():
    query = """
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER NOT NULL,
            place_id INTEGER NOT NULL,
            price INTEGER NOT NULL,
            area INTEGER NOT NULL,
            room_count INTEGER,
            first_seen_at TIMESTAMP NOT NULL,
            last_seen_at TIMESTAMP NOT NULL,
            PRIMARY KEY (id)
        );
    """

    try:
        g.db_cursor.execute(query)
        g.db.commit()

    except:
        g.db.rollback()


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

    raise Exception("this is an exception")

    # init database
    init_database()

    # get palaces ids
    places_ids = get_palaces_ids()
    logging.error(f"places to collect: {places_ids}")

    listings = []
    for place_id in places_ids:
        logging.error(f"read place: {place_id}")

        for page in count():
            logging.error(f"read page: {page} of {place_id}")

            url = f"{LISTINGS_API_URL}/{place_id}?page={page}"

            try:
                response = requests.get(url)
            except Exception as err:
                logging.error(f"listing api failed, error: {err}")
                # TODO implement retry mechanism
                break
            if response.status_code == 416:
                logging.error("no more page retrieve next place")
                break
            elif response.status_code != 200:
                logging.error(
                    f"listing api failed, http status : {response.status_code}"
                )
                break

            items = response.json()

            # TODO validate response schema
            # try:
            #   validate(items, LISTINGS_API_RESPONSE_SCHEMA)
            # except ValidationError as err:
            # logging.err(f"reponse schema validation failed: {err}")

            logging.error(f"number of items: {len(items)}")
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

            if len(items) < LISTINGS_API_PAGE_SIZE:
                logging.error("no more page retrieve next place")
                break

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

        psycopg2.extras.execute_batch(g.db_cursor, query, listings, page_size=100)
        g.db.commit()
        logging.error(f"listings updated successfully")
