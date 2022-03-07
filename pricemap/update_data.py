import json
import logging
import re
from datetime import datetime
from itertools import count

from flask import g, make_response

from jsonschema import validate, ValidationError

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
    logging.debug(f"geo_places: {geo_places}")
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

    g.db_cursor.execute(query)
    g.db.commit()


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

    if area == 0:
        logging.error("area equal to 0")
        raise ValueError("area equal to 0")

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

    logging.info(f"update listings: {update_time}")

    # init database
    try:
        init_database()
    except Exception as err:
        logging.error(f"init db failed: {err}")
        return make_response("init db failed", 500)

    # get palaces ids
    places_ids = get_palaces_ids()
    logging.debug(f"places to collect: {places_ids}")

    listings = []
    for place_id in places_ids:
        logging.debug(f"read place: {place_id}")

        for page in count():
            logging.debug(f"read page: {page} of {place_id}")

            url = f"{LISTINGS_API_URL}/{place_id}?page={page}"

            try:
                response = requests.get(url)
            except Exception as err:
                logging.error(f"listing api failed, error: {err}")
                return make_response("listings api unavailable", 503)

            if response.status_code == 416:
                logging.debug("no more page retrieve next place")
                break
            elif response.status_code != 200:
                logging.error(
                    f"listings api failed, http status : {response.status_code}"
                )
                return make_response("listings api failed", 503)

            items = response.json()

            # validate response schema
            try:
                validate(items, LISTINGS_API_RESPONSE_SCHEMA)
            except ValidationError as err:
                logging.err(f"listings api reponse schema validation failed: {err}")
                return make_response("bad listings api response", 503)

            logging.debug(f"number of items: {len(items)}")
            for item in items:
                try:
                    listing = extract_listing(item)
                except Exception as err:
                    logging.error(
                        f"invalid listing from api: {json.dumps(item)}, error: {err}"
                    )
                    # ignore invalid listing and continue
                    continue

                listing["place_id"] = place_id
                listing["first_seen_at"] = update_time
                listing["last_seen_at"] = update_time
                listings.append(listing)

            if len(items) < LISTINGS_API_PAGE_SIZE:
                logging.debug("no more page retrieve next place")
                break

    if listings:
        logging.info("update database")
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
        logging.info(f"listings updated successfully")
