import logging

from flask import Flask, g, render_template

import psycopg2

from pricemap.blueprints.api import api
from pricemap.update_data import update

app = Flask(__name__)
app.config.from_object("settings")
app.register_blueprint(api, url_prefix="/api")


@app.before_request
def before_request():
    """Before every requests, connect to database in case of any disconnection."""
    logging.error("before_request")

    if not hasattr(app, "_request_counter"):
        app._request_counter = 0
    logging.error(f"cntr:{app._request_counter}")
    if not hasattr(app, "db") or app.db.closed or app._request_counter == 10000:
        if hasattr(app, "db"):
            logging.error("closedb")
            app.db.close()
        logging.error("connect db")
        app.db = psycopg2.connect(**app.config["DATABASE"])
        app._request_counter = 0
    app._request_counter += 1
    g.db = app.db
    g.db_cursor = g.db.cursor(cursor_factory=psycopg2.extras.DictCursor)


@app.route("/")
def index():
    return render_template("index.html")


@app.teardown_request
def teardown_request(error=None):
    logging.error("teardown_request")


@app.route("/update_data")
def update_data():
    """Update the data."""
    update()
    return "", 200
