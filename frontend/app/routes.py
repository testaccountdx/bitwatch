from flask import render_template
from app import app
import config
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import psycopg2
from flask import request

# PostgreSQL configuration details
user = config.FLASK_CONFIG['PG_USER']
host = config.FLASK_CONFIG['PG_HOST']
port = config.FLASK_CONFIG['PG_PORT']
dbname = config.FLASK_CONFIG['PG_DB']
password = config.FLASK_CONFIG['PG_PASSWORD']

con = None
con = psycopg2.connect(host=host, port=port, database=dbname, user=user, password=password)


@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')


@app.route('/explorer')
def explorer():
    # pull input_addr from input fields
    input_addr = request.args.get('input_addr')

    # run query
    # input_address = "1GwmNuMxUvT1H1uR9NfEtHaqQPishcs1pJ"
    sql_query = "SELECT id, component FROM cluster100k WHERE component = (SELECT MIN(component) FROM cluster100k WHERE id = '{}');".format(input_addr)

    query_results = pd.read_sql_query(sql_query, con)

    statistics = []
    statistics.append(dict(label='No. of associated addresses', value=query_results.shape[0]))

    addresses = []
    for i in range(0, query_results.shape[0]):
        addresses.append(dict(number=i+1, address=query_results.iloc[i]['id']))

    return render_template('explorer.html', addresses=addresses, statistics=statistics)


@app.route('/clustogram')
def clustogram():
    return render_template('clustogram.html')

