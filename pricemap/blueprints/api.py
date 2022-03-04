#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import operator

from flask import Blueprint, jsonify, g, make_response

import psycopg2.extras

api = Blueprint("api", __name__)

@api.route("/geoms")
def geoms():

    SQL = """
            SELECT
                ST_ASGEOJSON(geom) as geom,
                cog,
                avg(price/area) as price
            FROM geo_place
            JOIN listings ON geo_place.id = listings.place_id
            group by (cog, geom)
            ;"""

    cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(SQL)

    response = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": json.loads(row["geom"]),
                "properties": {"cog": row["cog"], "price": row["price"]},
            }
            for row in cursor
            if row[0]
        ],
    }

    logging.error(f"response: {response}")
    return jsonify(response)


@api.route("/get_price/<path:cog>")
def get_price(cog):
    """
    Return the volumes distribution for the given cog in storage format
    """

    logging.error(f"get_price: cog: {cog}")

    RANGES = [(6000, 8000), (8000, 10000), (10000, 14000)]

    query = "select id from geo_place where cog=%(cog)s"
    cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query, {"cog": cog})
    geo_place = cursor.fetchone()
    logging.error(f"geo_place:{geo_place}")
    if geo_place is None:
        logging.error(f"place:{cog} not found")
        return make_response("place not found", 404)
    place_id = geo_place["id"]
    logging.error(f"place_id:{place_id}")

    query = " \nUNION ".join(
        [
            f"(select 0 as range, COUNT(price) AS count FROM listings where place_id=%(place_id)s and price/area < {RANGES[0][0]})",
            *[
                f"(select {range_number+1} as range, COUNT(price) AS count FROM listings where place_id=%(place_id)s and price/area BETWEEN {start_range} AND {end_range})"
                for range_number, (start_range, end_range) in enumerate(RANGES)
            ],
            f"(select {len(RANGES)+1} as range, COUNT(price) AS count FROM listings where place_id=%(place_id)s and price/area > {RANGES[-1][1]})",
        ]
    )

    logging.error(f"query: {query}")

    cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query, {"place_id": place_id})
    rows = cursor.fetchall()

    volumes = [row["count"] for row in sorted(rows, key=operator.itemgetter("range"))]

    labels = [
        f"< {RANGES[0][0]} €",
        *[f"{start_range}-{end_range} €" for start_range, end_range in RANGES],
        f"{RANGES[-1][1]} € >",
    ]

    serie_name = "Prix " + cog

    response = {
        "serie_name": serie_name,
        "volumes": volumes,
        "labels": labels,
    }

    logging.error(f"response: {response}")

    return jsonify(response)
