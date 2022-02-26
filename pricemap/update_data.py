import json

from flask import g, current_app, jsonify
import requests
import psycopg2
import psycopg2.extras

from datetime import datetime
import logging

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
            id INTEGER,
            place_id INTEGER,
            price INTEGER,
            area INTEGER,
            room_count INTEGER,
            seen_at TIMESTAMP,
            PRIMARY KEY (id, seen_at)
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


def decode_item(item):
    listing_id = item["listing_id"]
    try:
        room_count = (
            1
            if "Studio" in item["title"]
            else int(
                "".join([s for s in item["title"].split("pièces")[0] if s.isdigit()])
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
            item["title"].split("-")[1].replace(" ", "").replace("\u00a0m\u00b2", "")
        )
    except:
        area = 0

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

            for item in response.json():
                listing = decode_item(item)
                listing["geom"] = geom_id
                listing["seen_at"] = datetime.now()
                listings.append(listing)

    logging.error(f"listings: {listings}")

    query = """
                INSERT INTO listings VALUES(
                    %(listing_id)s,
                    %(geom)s,
                    %(price)s,
                    %(area)s,
                    %(room_count)s,
                    %(seen_at)s
                )
        """

    psycopg2.extras.execute_batch(db_cursor, query, listings)

    return

    for listing in listings:
        logging.error(f"insert query: {listing['listing_id']}")
        db_cursor.execute(
            query,
            {
                "listing_id": listing["listing_id"],
                "geom": listing["geom"],
                "price": listing["price"],
                "area": listing["area"],
                "room_count": listing["room_count"],
                "seen_at": listing["seen_at"],
            },
        )
        g.db.commit()
    return
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

                query = f"""
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
                    SET last_seen_at = %(last_seen_at)s;
                """

                db_cursor.execute(
                    query,
                    {
                        "listing_id": listing_id,
                        "geom": geom,
                        "price": price,
                        "area": area,
                        "room_count": room_count,
                        "first_seen_at": now,
                        "last_seen_at": now,
                    },
                )

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
