import jaydebeapi

# Hive JDBC driver class name
driver_class = "org.apache.hive.jdbc.HiveDriver"

def handler(request, jsonify):
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

        username = username or ""
        password = password or ""

        multi_line_sql = data['query']

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

        # Split the multi-line SQL query into individual statements
        multi_line_sql = multi_line_sql.replace('\n', '')
        sql_statements_raw = multi_line_sql.split(';')
        original_lines = [item for item in sql_statements_raw if item != '']
        sql_statements = [
            line for line in original_lines if not line.startswith('--')]
        print(sql_statements)

        # Execute each SQL statement
        for index, statement in enumerate(sql_statements):
            if statement.strip():
                cursor.execute(statement)
                try:
                    # Check the type of query to determine the operation
                    if statement.strip().upper().startswith("SELECT"):
                        # If it's a SELECT query, fetch the results
                        cursor.execute(statement)

                        result = cursor.fetchall()

                        try:
                            field_names = [desc[0]
                                           for desc in cursor.description]
                            if field_names:
                                fn_rm_prefix = [field.split(".")[1]
                                                for field in field_names]

                                # Check if the current iteration is the last element in the list
                                if index == len(sql_statements) - 1:
                                    cursor.close()
                                    connection.close()
                                    return jsonify({"datas": result, "fields": fn_rm_prefix}), 200
                        except:
                            pass

                        # Check if the current iteration is the last element in the list
                        if index == len(sql_statements) - 1:
                            cursor.close()
                            connection.close()
                            return jsonify(result), 200

                    elif statement.strip().upper().startswith("SHOW"):
                        # If it's a SELECT query, fetch the results
                        cursor.execute(statement)
                        results = cursor.fetchall()
                        flattened_results = [
                            item for sublist in results for item in sublist]
                        if index == len(sql_statements) - 1:
                            cursor.close()
                            connection.close()
                            return jsonify({"datas": flattened_results, }), 200
                    else:
                        # For non-SELECT queries (INSERT, UPDATE, DELETE, etc.), execute and commit
                        # cursor.execute(statement)

                        # try:
                        #     connection.commit()
                        # except:
                        #     pass

                        # Get the number of affected rows
                        num_affected_rows = cursor.rowcount

                        if index == len(sql_statements) - 1:
                            cursor.close()
                            connection.close()
                            return jsonify({"message": f"Query executed successfully. Affected rows: {num_affected_rows}"}), 200
                except Exception as e:
                    err = str(e)
                    print(err)
                    if "org.apache.hive.service.cli.HiveSQLException: Error while compiling statement: FAILED: " in err:
                        err = err.replace(
                            "org.apache.hive.service.cli.HiveSQLException: Error while compiling statement: FAILED: ", "")
                    return jsonify({"error": err}), 400

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
