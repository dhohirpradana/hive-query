from flask import Flask, request, jsonify
import jaydebeapi

app = Flask(__name__)

# Hive JDBC driver class name
driver_class = "org.apache.hive.jdbc.HiveDriver"

# JDBC connection URL
# jdbc_url = "jdbc:hive2://10.10.65.5:10000"

# Username and password (leave empty if not required)
# username = "hive"
# password = "hive"


@app.route('/hive_query', methods=['POST'])
def execute_hive_query():
    try:
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({"error": "Query not provided in request body"}), 400

        # Retrieve host, port, username, and password from query parameters
        host = request.args.get('host')
        port = request.args.get('port')
        username = request.args.get('username')
        password = request.args.get('password')

        if not host:
            return jsonify({"error": "Host not provided in query parameters"}), 400

        if not port:
            return jsonify({"error": "Port not provided in query parameters"}), 400

        # if not username:
        #     return jsonify({"error": "Username not provided in query parameters"}), 400

        # if not password:
        #     return jsonify({"error": "Password not provided in query parameters"}), 400

        username = username or ""
        password = password or ""

        query = data['query']

        jdbc_url = f"jdbc:hive2://{host}:{port}"

        # Create a JDBC connection
        connection = jaydebeapi.connect(
            jclassname=driver_class,
            url=jdbc_url,
            driver_args=[username, password],
            # Replace with the actual path to your Hive JDBC driver JAR file
            jars="hive-jdbc-uber-2.6.5.0-292.jar",
        )

        # Create a cursor
        cursor = connection.cursor()

        # Check the type of query to determine the operation
        if query.strip().upper().startswith("SELECT"):
            # If it's a SELECT query, fetch the results
            cursor.execute(query)
            results = cursor.fetchall()
            field_names = [desc[0] for desc in cursor.description]
            fn_rm_prefix = [field.split(".")[1] for field in field_names]
            cursor.close()
            connection.close()
            return jsonify({"fields": fn_rm_prefix, "datas": results, }), 200
        elif query.strip().upper().startswith("SHOW"):
            # If it's a SELECT query, fetch the results
            cursor.execute(query)
            results = cursor.fetchall()
            flattened_results = [
                item for sublist in results for item in sublist]
            cursor.close()
            connection.close()
            return jsonify({"datas": flattened_results, }), 200
        else:
            # For non-SELECT queries (INSERT, UPDATE, DELETE, etc.), execute and commit
            cursor.execute(query)

            try:
                connection.commit()
            except:
                pass

            # Get the number of affected rows
            num_affected_rows = cursor.rowcount

            cursor.close()
            connection.close()
            return jsonify({"message": f"Query executed successfully. Affected rows: {num_affected_rows}"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
