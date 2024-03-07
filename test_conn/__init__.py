import jaydebeapi

# Hive JDBC driver class name
driver_class = "org.apache.hive.jdbc.HiveDriver"


def handler(request, jsonify):
    try:
        # Retrieve host, port, username, and password from query parameters
        host = request.args.get('host')
        port = request.args.get('port')
        username = request.args.get('username')
        password = request.args.get('password')

        if not host:
            return jsonify({"error": "Host not provided in query parameters"}), 400

        if not port:
            return jsonify({"error": "Port not provided in query parameters"}), 400

        username = username or ""
        password = password or ""

        jdbc_url = f"jdbc:hive2://{host}:{port}"

        # Create a JDBC connection
        connection = jaydebeapi.connect(
            jclassname=driver_class,
            url=jdbc_url,
            driver_args=[username, password],
            # Replace with the actual path to your Hive JDBC driver JAR file
            jars="hive-jdbc-uber-2.6.5.0-292.jar",
        )

        return jsonify({"message": "Hive connection successfully"}), 200

    except Exception as e:
        err = str(e)
        print(err)
        return jsonify({"message": err}), 400
