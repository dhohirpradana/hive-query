import jaydebeapi

# Hive JDBC driver class name
driver_class = "org.apache.hive.jdbc.HiveDriver"


def table_detail(table_name, request, jsonify):
    try:
        # Retrieve host, port, username, and password from query parameters
        host = request.args.get('host')
        port = request.args.get('port')
        db = request.args.get('db')
        username = request.args.get('username')
        password = request.args.get('password')

        if not host:
            return jsonify({"error": "Host not provided in query parameters"}), 400

        if not port:
            return jsonify({"error": "Port not provided in query parameters"}), 400

        if not db:
            return jsonify({"error": "Db not provided in query parameters"}), 400

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

        # Create a cursor
        cursor = connection.cursor()

        # USE DATABASE
        cursor.execute(f"USE {db}")

        # Retrieve table metadata
        cursor.execute(f"DESCRIBE {table_name}")

        # Fetch and print column information for the current table
        columns = cursor.fetchall()

        fields = []

        # Print column information
        for column_info in columns:
            ta = {"name": column_info[0], "type": column_info[1]}
            fields.append(ta)

        return jsonify({"table": f"{db}.{table_name}", "fields": fields, "total": len(fields)}), 200
    except Exception as e:
        err = str(e)
        print(err)
        if "org.apache.hive.service.cli.HiveSQLException: Error while compiling statement: FAILED: " in err:
            err = err.replace(
                "org.apache.hive.service.cli.HiveSQLException: Error while compiling statement: FAILED: ", "")
            # Split the string at the first colon
            # err = err.split(':', 1)
            # err = err[1].strip()
        return jsonify({"error": err}), 400


def handler(request, jsonify):
    try:
        # Retrieve host, port, username, and password from query parameters
        host = request.args.get('host')
        port = request.args.get('port')
        db = request.args.get('db')
        username = request.args.get('username')
        password = request.args.get('password')

        if not host:
            return jsonify({"error": "Host not provided in query parameters"}), 400

        if not port:
            return jsonify({"error": "Port not provided in query parameters"}), 400

        if not db:
            return jsonify({"error": "Db not provided in query parameters"}), 400

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

        # Create a cursor
        cursor = connection.cursor()

        # Replace 'your_table_name' with the actual table name
        cursor.execute(f"SHOW TABLES FROM {db}")

        # Fetch the table names
        tables = cursor.fetchall()

        print("TABLES: ", tables)

        dts = {}
        table_names = []

        # Iterate through the tables and fetch column information for each
        for table in tables:
            table_name = table[0]
            table_names.append(table_name)
            dts[table_name] = []

            # # Retrieve table metadata
            # cursor.execute(f"DESCRIBE {db}.{table_name}")

            # # Fetch and print column information for the current table
            # columns = cursor.fetchall()

            # # Print column information
            # for column_info in columns:
            #     ta = {"name": column_info[0], "type": column_info[1]}
            #     dts[table_name].append(ta)

        print(dts)
        # return jsonify({"tables": table_names, "fields": dts}), 200
        return jsonify({"database": db, "tables": table_names, "total": len(table_names)}), 200

    except Exception as e:
        err = str(e)
        print(err)
        if "org.apache.hive.service.cli.HiveSQLException: Error while compiling statement: FAILED: " in err:
            err = err.replace(
                "org.apache.hive.service.cli.HiveSQLException: Error while compiling statement: FAILED: ", "")
            # Split the string at the first colon
            # err = err.split(':', 1)
            # err = err[1].strip()
        return jsonify({"error": err}), 400
