#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function, unicode_literals

from flask import Blueprint, jsonify, g
import operator

import psycopg2.extras
import json
import logging

api = Blueprint("api", __name__)


@api.route("/geoms")
def geoms():
    # TODO: you can tweak the query and/or the code if you think it's needed :)
    SQL = """
            SELECT
                ST_ASGEOJSON(geom) as geom,
                cog,
                sum(price) / sum(area) as price
            FROM geo_place
            JOIN listings ON geo_place.id = listings.place_id
            group by (cog, geom)
            ;"""

    cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(SQL)

    geoms = {
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

    """
    for row in cursor:
        logging.error(f"row from db: {row}")
        if not row[0]:
            continue
        geometry = {
            "type": "Feature",
            "geometry": json.loads(row["geom"]),
            "properties": {"cog": row["cog"], "price": row["price"]},
        }
        geoms["features"].append(geometry)
    """
    return jsonify(geoms)


@api.route("/get_price/<path:cog>")
def get_price(cog):
    """
    Return the volumes distribution for the given cog in storage format
    """
    # new implementation
    logging.error(f"get_price:{cog}")
    # TODO check if exists

    query = "select exists(select 1 from geo_place where cog=%(cog)s)"
    cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query, {"cog": cog})
    place = cursor.fetchone()
    logging.error(f"--->place:{place}, {dict(place)}")

    RANGES = [(6000, 8000), (8000, 10000), (10000, 14000)]

    query = " \nUNION ".join(
        [
            f"(select 0 as range, COUNT(price) AS count FROM listings WHERE price/area < {RANGES[0][0]})",
            *[
                f"(select {range_number+1} as range, COUNT(price) AS count FROM listings WHERE price/area BETWEEN {start_range} AND {end_range})"
                for range_number, (start_range, end_range) in enumerate(RANGES)
            ],
            f"(select {len(RANGES)+1} as range, COUNT(price) AS count FROM listings WHERE price/area > {RANGES[-1][1]})",
        ]
    )

    logging.error(f"query:{query}")

    cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        logging.error("---->")
        logging.error(f"row:{row}")
        logging.error(f"rowdict:{dict(row)}")

    volumes = [row["count"] for row in sorted(rows, key=operator.itemgetter("range"))]

    labels = [
        f"< {RANGES[0][0]} €",
        *[f"{start_range}-{end_range} €" for start_range, end_range in RANGES],
        f"{RANGES[-1][1]} € >",
    ]

    logging.error(f"labels:{labels}")

    serie_name = "Prix " + cog

    response = {
        "serie_name": serie_name,
        "volumes": volumes,
        "labels": labels,
    }

    logging.error(f"response:{response}")

    return jsonify(response)

    #####################################
    # TODO : maybe we can do a better histogram (better computation, better volume and labels, etc.)
    serie_name = "Prix " + cog
    labels = {
        "0-6000": 0,
        "6000-8000": 0,
        "8000-10000": 0,
        "10000-14000": 0,
        "14000-100000": 0,
    }

    for label in labels:
        min_price = label.split("-")[0]
        max_price = label.split("-")[1]
        SQL = f"""
            SELECT
                 ST_ASGEOJSON(geom) as geom,
                 cog,
                 area,
                 price
             FROM geo_place
             JOIN listings ON geo_place.id = listings.place_id
             WHERE area != 0 AND price / area > {min_price} AND price / area < {max_price}
             ;"""
        cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(SQL)
        rows = cursor.fetchall()
        labels[label] = len(rows)

    response = {
        "serie_name": serie_name,
        "volumes": list(labels.values()),
        "labels": list(labels.keys()),
    }
    response = {}
    return jsonify(response)
