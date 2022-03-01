#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function, unicode_literals

from flask import Blueprint, jsonify, g

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
            group by (cog)
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


[
    {
        "type": "MultiPolygon",
        "coordinates": [
            [
                [
                    [2.28444260174361, 48.885631630165],
                    [2.28547810067507, 48.8864414493875],
                    [2.28564160555692, 48.8865674215181],
                    [2.28611863671888, 48.8868014468181],
                    [2.28636396299489, 48.8869274544811],
                    [2.28640485389413, 48.8869454581322],
                    [2.28864016556006, 48.8880345505883],
                    [2.29113452872304, 48.8892855731789],
                    [2.29146166953554, 48.8894475737734],
                    [2.29189800826063, 48.8895017040574],
                    [2.29202072614182, 48.8895197381261],
                    [2.29212981512525, 48.889528773957],
                    [2.29237525939546, 48.8895558487397],
                    [2.29263434109363, 48.8895829283174],
                    [2.29290705222162, 48.8896190053227],
                    [2.29364338844241, 48.8897002230804],
                    [2.29422973326632, 48.889763394299],
                    [2.29446154262208, 48.8897904594683],
                    [2.29452972098338, 48.8897994776905],
                    [2.29500697020547, 48.8898626041063],
                    [2.29502061508098, 48.8898536164184],
                    [2.2954568100668, 48.8900875876203],
                    [2.29605659791853, 48.8903935580352],
                    [2.29740612957773, 48.8911044617752],
                    [2.29786961494122, 48.8913474259956],
                    [2.29793777472669, 48.8913834203614],
                    [2.29800593461092, 48.8914194146866],
                    [2.29855121723998, 48.891707367825],
                    [2.29902836546407, 48.8919323465098],
                    [2.29965548015895, 48.892229314427],
                    [2.30024170852346, 48.8924992874922],
                    [2.30058253819523, 48.8926612663902],
                    [2.30279117231591, 48.8937051096734],
                    [2.30360920827965, 48.8940920405301],
                    [2.30564071241154, 48.8950638346519],
                    [2.3075086879849, 48.8959366302357],
                    [2.30952682496657, 48.896692526302],
                    [2.31144956677396, 48.8974123943335],
                    [2.31225412795499, 48.8977273184233],
                    [2.31307237379383, 48.8979702979171],
                    [2.31337239884181, 48.8980602882031],
                    [2.31342694818414, 48.8980782850299],
                    [2.31369969974672, 48.8981592760465],
                    [2.31372697452658, 48.8981682743851],
                    [2.31378152411594, 48.8981862710427],
                    [2.31380879892544, 48.8981952693618],
                    [2.31384971327368, 48.8982042704646],
                    [2.31387698810541, 48.8982132687674],
                    [2.31408155175284, 48.8982762594671],
                    [2.31412246203565, 48.8982942531994],
                    [2.31492708672997, 48.8985372166571],
                    [2.31518620361534, 48.8986182014646],
                    [2.3152816691982, 48.8986451980159],
                    [2.31540440630058, 48.8986901851529],
                    [2.31554078722483, 48.8987261820195],
                    [2.31562261352822, 48.8987531756976],
                    [2.31575899478351, 48.8987891723038],
                    [2.31578626640179, 48.8988071628781],
                    [2.31586809683756, 48.8988251636533],
                    [2.31589537237578, 48.8988341614744],
                    [2.31604538992932, 48.8988791530106],
                    [2.3180229121761, 48.8994910078297],
                    [2.31847297738312, 48.8996259734698],
                    [2.31850025383282, 48.8996349706693],
                    [2.31856843655818, 48.8996799454576],
                    [2.31858207649245, 48.8996799476832],
                    [2.31979577196392, 48.9004175428758],
                    [2.31985031993194, 48.9004535220916],
                    [2.32020489289208, 48.9006604081779],
                    [2.32032763025645, 48.9007323682103],
                    [2.32035490154251, 48.900759350422],
                    [2.3218689532628, 48.9008045274875],
                    [2.32327388569141, 48.9008496712297],
                    [2.32335572471497, 48.9008586739164],
                    [2.3234512065218, 48.9008586854614],
                    [2.32354668832867, 48.9008586969267],
                    [2.33012126841041, 48.9010301563653],
                    [2.32943957297556, 48.8989887628057],
                    [2.32896240918773, 48.8976218347313],
                    [2.32826717418977, 48.8955534550618],
                    [2.32742206342681, 48.8930444148064],
                    [2.32725849985362, 48.8925767788502],
                    [2.32684961753267, 48.8913447388564],
                    [2.32659066774201, 48.8905713405349],
                    [2.32624994637177, 48.889600093817],
                    [2.32589562397027, 48.888529924749],
                    [2.32573210002086, 48.8880173229959],
                    [2.3255685836635, 48.8874867355586],
                    [2.32633259716801, 48.8858231565087],
                    [2.3267555104399, 48.8849059379348],
                    [2.32741030787893, 48.8835121234631],
                    [2.32715124073002, 48.8834761300148],
                    [2.32668765514596, 48.8833771682199],
                    [2.32656494179414, 48.88335017866],
                    [2.32652403737315, 48.8833411821107],
                    [2.32547415875977, 48.883125253545],
                    [2.32543325472141, 48.8831162566053],
                    [2.32539234855705, 48.8831162523769],
                    [2.32531054053958, 48.8830982584242],
                    [2.32430157596515, 48.8828913160675],
                    [2.32419250042804, 48.8828643255009],
                    [2.32416523215299, 48.8828553296614],
                    [2.32404251907477, 48.8828373301184],
                    [2.32400161553261, 48.8828283326661],
                    [2.32396071200527, 48.8828193351992],
                    [2.32340169694866, 48.8827023635008],
                    [2.32336079363362, 48.8826933658191],
                    [2.32331989033339, 48.8826843681228],
                    [2.3232653517809, 48.8826753687469],
                    [2.32323808377738, 48.8826663726862],
                    [2.32051121534483, 48.8821174500623],
                    [2.32042941073922, 48.8820994526147],
                    [2.32030670394191, 48.8820724563334],
                    [2.31959772969319, 48.8819284655404],
                    [2.31875241935545, 48.8817574702086],
                    [2.31785257846789, 48.8815774666631],
                    [2.31681640970792, 48.8813614608553],
                    [2.31680277480162, 48.8813614584191],
                    [2.31662553592846, 48.8813254556976],
                    [2.3164755595766, 48.8813074431078],
                    [2.31254895199722, 48.8808210553327],
                    [2.31160820933361, 48.8807129355687],
                    [2.3091132270663, 48.8804065962322],
                    [2.30881329574619, 48.8803525656823],
                    [2.30858152778384, 48.8803165369068],
                    [2.30548688850814, 48.8796592501199],
                    [2.30306031701495, 48.8791369721172],
                    [2.30094733606618, 48.8786776916539],
                    [2.29812553714662, 48.8780742480836],
                    [2.29794849408622, 48.8778223910057],
                    [2.295987453445, 48.8751148892457],
                    [2.29527935470495, 48.8741344242528],
                    [2.2952521192356, 48.8740984433434],
                    [2.29514318536593, 48.8739455269171],
                    [2.29502063460931, 48.8737746199042],
                    [2.29491154905678, 48.8738015577295],
                    [2.29451608879134, 48.873927308734],
                    [2.29399789914581, 48.8740889829013],
                    [2.29297514273945, 48.8744123294662],
                    [2.29046590977295, 48.8752206753616],
                    [2.28994769346884, 48.8753823311581],
                    [2.28917036269206, 48.8756248104454],
                    [2.28797024645211, 48.8760109865779],
                    [2.28716561107667, 48.8762714259125],
                    [2.28390610316404, 48.8772951220531],
                    [2.28375608312742, 48.8773400155756],
                    [2.28331964547506, 48.8774836941184],
                    [2.27977350640139, 48.8786420277525],
                    [2.28014053378196, 48.8795954409284],
                    [2.28064255010592, 48.8817539454735],
                    [2.28066963592058, 48.8819158279974],
                    [2.28069675259827, 48.8820507323449],
                    [2.28071031610553, 48.8821136881546],
                    [2.2808595373526, 48.8827882165107],
                    [2.28088671618977, 48.8828691644731],
                    [2.2813499125241, 48.8832291010782],
                    [2.28136353783112, 48.8832381004693],
                    [2.2816904964335, 48.883499048987],
                    [2.28184035653033, 48.8836160272007],
                    [2.28444260174361, 48.885631630165],
                ]
            ]
        ],
    },
    "75117",
    12480,
]
