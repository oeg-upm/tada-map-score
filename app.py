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


@app.route('/test')
def test_combine():
    import socket
    if request.args.get('dest') is None:
        return "combine ip: "+str(socket.gethostbyname('combine'))+ " - " + str(socket.gethostbyname('score'))

    # else:
    #     return "dest: <%s>" % request.args.get('dest')
    import requests
    try:
        r = requests.get(request.args.get('dest'))
        return r.content
    except Exception as e:
        return str(e)
    return 'Hello World! graph'


@app.route('/')
def hello_world():
    return 'Hello World! graph'


@app.route('/score', methods=['POST'])
def score():
    logger.debug("\nin score")
    # print(request.files)
    uploaded_file = request.files['file_slice']
    # print("post data:" )
    # print request.form
    # print("file content: ")
    # print(uploaded_file.read())
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
    # print("address: "+str(request.form['addr']+"/add"))
    logger.debug("\nsending to combine: "+str(get_params))
    r = requests.get(request.form['addr']+"/add", params=get_params)
    if r.status_code != 200:
        logger.debug("error: "+r.content)
    return 'data received and processed'


@app.route('/register', methods=['GET'])
def register():
    table_name = request.args.get('table')
    # Bite.create(table=table_name, slice=0, column=0)
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




