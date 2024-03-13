import re

from connection import handler as connection_handler


def handler(request, jsonify):
    try:
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({"error": "Query not provided in request body"}), 400

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

        username = username or ""
        password = password or ""

        multi_line_sql = data['query']

        # Split the multi-line SQL query into individual statements
        multi_line_sql = multi_line_sql.replace('\n', ';')
        sql_statements_raw = multi_line_sql.split(';')
        original_lines = [item for item in sql_statements_raw if item != '']
        sql_statements = [
            line for line in original_lines if not line.startswith('--')]
        print(sql_statements)

        if not db:
            db = 'default'

        # Execute each SQL statement
        cursor, connection = connection_handler(host, port, username, password, db)
        for index, statement in enumerate(sql_statements):
            if statement.strip():
                try:
                    if statement.strip().upper().startswith("USE"):
                        # Use regular expression to extract the word after "USE"
                        pattern = r"USE\s+(\w+)"

                        # Match and extract the word from both strings
                        match = re.search(pattern, statement)
                        if match:
                            db = match.group(1)
                            print(db)

                        # Create a cursor
                        cursor, connection = connection_handler(host, port, username, password, db)
                        cursor.execute(statement)

                        # Get the number of affected rows
                        num_affected_rows = cursor.rowcount

                        if index == len(sql_statements) - 1:
                            cursor.close()
                            connection.close()
                            return jsonify(
                                {"message": f"Query executed successfully. Affected rows: {num_affected_rows}"}), 200
                    # Check the type of query to determine the operation
                    # If it's a SELECT query, fetch the results
                    if statement.strip().upper().startswith("SELECT"):
                        cursor.execute(statement)
                        result = cursor.fetchall()

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

                        # Check if the current iteration is the last element in the list
                        if index == len(sql_statements) - 1:
                            cursor.close()
                            connection.close()
                            return jsonify(result), 200

                    # If it's a SHOW query, fetch the results
                    elif statement.strip().upper().startswith("SHOW"):
                        cursor.execute(statement)
                        results = cursor.fetchall()
                        flattened_results = [
                            item for sublist in results for item in sublist]
                        if index == len(sql_statements) - 1:
                            cursor.close()
                            connection.close()
                            return jsonify({"datas": flattened_results, }), 200
                    # For non-SELECT queries (INSERT, UPDATE, DELETE, etc.), execute and commit
                    else:
                        cursor.execute(statement)

                        # try:
                        #     connection.commit()
                        # except:
                        #     pass

                        # Get the number of affected rows
                        num_affected_rows = cursor.rowcount

                        if index == len(sql_statements) - 1:
                            cursor.close()
                            connection.close()
                            return jsonify(
                                {"message": f"Query executed successfully. Affected rows: {num_affected_rows}"}), 200
                except Exception as e:
                    err = str(e)
                    print(err)
                    if "org.apache.hive.service.cli.HiveSQLException: Error while compiling statement: FAILED: " in err:
                        err = err.replace(
                            "org.apache.hive.service.cli.HiveSQLException: Error while compiling statement: FAILED: ",
                            "")
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
