import os
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
from models import Bite, database, create_tables
from flask import Flask, g, request, render_template
from werkzeug.utils import secure_filename
from graph import type_graph
import requests

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World! This is score'


@app.route('/score', methods=['POST'])
def score():
    logger.debug("\nin score")
    uploaded_file = request.files['file_slice']
    table_name = request.form['table']
    b = Bite(table=table_name, slice=request.form['slice'], column=request.form['column'],
             addr=request.form['addr'])
    b.save()
    get_params = {
        'table': b.table,
        'column': b.column,
        'slice': b.slice,
        'addr': b.addr,
        'total': request.form['total']
    }
    if b.addr != '':
        logger.debug("\nsending to combine: " + str(get_params))
        r = requests.get(b.addr+"/add", params=get_params)
        if r.status_code != 200:
            logger.debug("error: "+r.content)
    else:
        logger.debug("\nempty address of combine service: " + str(get_params))
    return 'data received and processed'


@app.route('/register', methods=['GET'])
def register():
    table_name = request.args.get('table')
    b = Bite(table=table_name, slice=0, column=0, addr="default")
    b.save()
    return 'Table: %s is added' % table_name


@app.route('/fetch', methods=['GET'])
def fetch():
    bites = """
    Bites
    <table>
            <tr>
            <td>Table</td>
            <td>Column</td>
            <td>Slice</td>
            <td>Address</td>

        </tr>
    """
    for bite in Bite.select():
        bites += "<tr>"
        bites += "<td>%s</td>\n" % bite.table
        bites += "<td>%d</td>\n" % bite.column
        bites += "<td>%d</td>\n" % bite.slice
        bites += "<td>%s</td>\n" % bite.addr
        bites += "</tr>"
    bites += "</table>"
    return bites


@app.before_request
def before_request():
    g.db = database
    g.db.connect()


@app.after_request
def after_request(response):
    g.db.close()
    return response


if __name__ == '__main__':
    create_tables()
    if 'port' in os.environ:
        app.run(debug=True, host='0.0.0.0', port=int(os.environ['port']))
    else:
        app.run(debug=True, host='0.0.0.0')




