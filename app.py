import os

from models import Bite, database, create_tables
from flask import Flask, g, request, render_template, jsonify
from werkzeug.utils import secure_filename
from graph import type_graph
import requests
import random
import string
import subprocess
from logger import get_logger

logger = get_logger(__name__)

app = Flask(__name__)


def get_random(size=4):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(size))


@app.route('/')
def hello_world():
    return 'Hello World! This is score'


@app.route('/score', methods=['POST'])
def score():
    logger.debug("\nin score")
    uploaded_file = request.files['file_slice']
    table_name = request.form['table']
    column = request.form['column']
    fname = table_name + "__" + get_random() + "__" + str(column) + ".tsv"
    fname = secure_filename(fname)
    b = Bite(table=table_name, slice=request.form['slice'], column=column,
             addr=request.form['addr'], fname=fname)
    b.save()
    uploaded_file.save(os.path.join('local_uploads', fname))
    get_params = {
        'table': b.table,
        'column': b.column,
        'slice': b.slice,
        'addr': b.addr,
        'total': request.form['total']
    }
    if app.testing:
        from score import parse_args
        logger.debug("will wait for the scoring to be done")
        parse_args(args=["--id", "%d" % b.id])
        return jsonify({'msg': 'scored'})
    else:
        logger.debug("will return and the scoring will run in a different thread")
        comm = "python score --id %s" % str(b.id)
        subprocess.Popen(comm, shell=True)
        return jsonify({'msg': 'scoring in progress'})


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
            <td>Fname</td>
            <td>Address</td>
        </tr>
    """
    for bite in Bite.select():
        bites += "<tr>"
        bites += "<td>%s</td>\n" % bite.table
        bites += "<td>%d</td>\n" % bite.column
        bites += "<td>%d</td>\n" % bite.slice
        bites += "<td>%s</td>\n" % bite.fname
        bites += "<td>%s</td>\n" % bite.addr
        bites += "</tr>"
    bites += "</table>"
    return bites


@app.before_request
def before_request():
    # if app.testing:
    #    print("\n\ntesting")
    # else:
    #     print("\n\nnot testing")
    g.db = database
    g.db.connect(reuse_if_open=True)


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




