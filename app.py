from flask import Flask, request, jsonify

from query import handler as hive_query_handler
from table import handler as table_handler, table_detail as table_detail_handler
from test_conn import handler as test_conn_handler

app = Flask(__name__)


@app.route('/hive_query', methods=['POST'])
def hive_query():
    return hive_query_handler(request, jsonify)


@app.route('/tables', methods=['GET'])
def table():
    return table_handler(request, jsonify)


@app.route('/table/<table_name>', methods=['GET'])
def table_detail(table_name):
    return table_detail_handler(table_name, request, jsonify)


@app.route('/test-connection', methods=['GET'])
def test_conn():
    return test_conn_handler(request, jsonify)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
