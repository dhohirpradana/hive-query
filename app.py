from flask import Flask, request, jsonify
from query import handler as hive_query
from table import handler as table

app = Flask(__name__)

@app.route('/hive_query', methods=['POST'])
def execute_hive_query():
    return hive_query(request, jsonify)

@app.route('/tables', methods=['GET'])
def tables():
    return table(request, jsonify)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
