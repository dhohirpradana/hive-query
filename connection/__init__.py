import jaydebeapi

# Hive JDBC driver class name
driver_class = "org.apache.hive.jdbc.HiveDriver"

def handler(host, port, username, password, db):
    jdbc_url = f"jdbc:hive2://{host}:{port}/{db}"

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
    return cursor, connection