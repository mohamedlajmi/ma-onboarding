import json

from flask import g, current_app, jsonify
import requests
import psycopg2
import psycopg2.extras
from parse import parse
from datetime import datetime
import logging

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


def get_geoms_ids():
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

    def to_int(string):
        return int(string.replace(" ", ""))

    # parse title
    title_formats = [
        "Appartement{room_count:to_int}pièces -{area:to_int}m²",
        "Studio -{area:to_int}m²",
        "Appartement -{area:to_int}m²",
    ]

    parse_title_results = (
        parse(title_format, item["title"], dict(to_int=to_int))
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
        logging.error("\nfailed to extract area\n")
        # raise exception because area is required
        raise ValueError("area not found")
    area = title_fields["area"]

    # parse price
    price_format = "{price:to_int}€"
    parse_price_result = parse(price_format, item["price"], dict(to_int=to_int))
    if not parse_price_result:
        logging.error("\nfailed to extract price\n")
        # raise exception because price is required
        raise ValueError("price not found")

    # extract price
    price = parse_price_result.named["price"]

    return {
        "listing_id": listing_id,
        "room_count": room_count,
        "price": price,
        "area": area,
    }


def decode_item(item):

    # logging.error(f"item: {json.dumps(item)}")

    # required: listing_id, price, area
    # not required:room_count

    # title format: "Appartement 3 pièces - 73 m²"
    # title format: "Studio - 16 m²"

    # price format: "2 150 000 €"

    listing_id = item["listing_id"]

    # room_count
    try:
        room_count = (
            1
            if "Studio" in item["title"]
            else int(
                "".join([s for s in item["title"].split("pièces")[0] if s.isdigit()])
            )
        )
    except:
        logging.error("\nfailed to extract room_count:\n")
        logging.error(f"{json.dumps(item)}")
        room_count = None

    try:
        price = int("".join([s for s in item["price"] if s.isdigit()]))
    except:
        logging.error("\nfailed to extract price\n")
        # raise exception because price is required
        raise ValueError("price not found")

    # area
    try:
        area = int(
            item["title"].split("-")[1].replace(" ", "").replace("\u00a0m\u00b2", "")
        )
    except:
        logging.error("\nfailed to extract area\n")
        # raise exception because area is required
        raise ValueError("area not found")

    return {
        "listing_id": listing_id,
        "room_count": room_count,
        "price": price,
        "area": area,
    }


def update():
    # init database
    init_database()
    geoms_ids = get_geoms_ids()
    logging.error(f"geoms_ids: {geoms_ids}")

    db_cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    update_time = datetime.now()

    listings = []
    for geom_id in geoms_ids:
        logging.error(f"read geom : {geom_id}")
        page = 0
        # more_data = True
        while True:
            page += 1
            logging.error(f"read page : {page}")
            url = f"http://listingapi:5000/listings/{geom_id}?page={page}"
            response = requests.get(url)

            # Break when finished
            if response.status_code == 416:
                logging.error("no more page retrieve next geom")
                break

            # TODO validate reponse schema here

            for item in response.json():
                try:
                    # listing = decode_item(item)
                    listing = extract_listing(item)
                except:
                    logging.error(f"invalid listing from api: {json.dumps(item)}")
                    # ignore it
                    continue

                listing["geom"] = geom_id
                listing["first_seen_at"] = update_time
                listing["last_seen_at"] = update_time
                listings.append(listing)

    # logging.error(f"listings: {listings}")

    if listings:
        query = """
                    INSERT INTO listings VALUES(
                        %(listing_id)s,
                        %(geom)s,
                        %(price)s,
                        %(area)s,
                        %(room_count)s,
                        %(first_seen_at)s,
                        %(last_seen_at)s
                    )
                    ON CONFLICT (id) DO UPDATE
                    SET last_seen_at = %(last_seen_at)s
            """

        psycopg2.extras.execute_batch(db_cursor, query, listings, page_size=100)
        g.db.commit()


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
