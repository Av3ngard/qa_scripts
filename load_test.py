import contextlib
import sys
import threading
import time

class DatabaseType(object):
    MSSQL = 1
    ORACLE = 2
    POSTGRESQL = 3
    MYSQL = 4

# ----------------------------------------------------------------------------------------------------

#g_database_type = DatabaseType.MSSQL
#g_hostname = "192.168.10.113"
#g_port = "1433"
#g_database = "ds_audit"
#g_login = "sa"
#g_password = "84218421"

# g_database_type = DatabaseType.ORACLE
# g_hostname = "192.168.10.113"
# g_port = "1521"
# g_database = "XEPDB1"
# g_login = "test_albert"
# g_password = "84218421"

#g_database_type = DatabaseType.POSTGRESQL
#g_hostname = "alb-pg-trail.cjuqg4i5vbuu.us-east-2.rds.amazonaws.com"
#g_port = "5432"
#g_database = "test"
#g_login = "postgres"
#g_password = "84218421"

g_database_type = DatabaseType.MYSQL
g_hostname = "albertqa-db.mysql.database.azure.com"
g_port = "3306"
g_database = "albertqa"
g_login = "albertqa"
g_password = "Armor-409"

g_connections_count = 10
g_queries_count = 1000
g_pause_time = 0
g_threads_count = 1

# ----------------------------------------------------------------------------------------------------

class ConnectionInfo(object):
    hostname = None
    port = None
    database = None
    login = None
    password = None

class Logger(object):
    def __init__(self, old_output):
        self.old_output = old_output

    def flush(self, *args, **kwargs):  # real signature unknown
        self.old_output.flush(*args, **kwargs)

    def write(self, *args, **kwargs):  # real signature unknown
        self.old_output.write(*args, **kwargs)

sys.stdout = Logger(sys.stdout)

def open_connection(database_type, connection_info: ConnectionInfo):
    def get_driver_like(pattern):
        import subprocess
        cat_process = subprocess.Popen(
            'cat /etc/odbcinst.ini', stdout=subprocess.PIPE, universal_newlines=True, shell=True
        )
        out, err = cat_process.communicate()
        assert cat_process.returncode == 0
        odbc_drivers = [line[1:-1] for line in str(out).splitlines() if line.startswith('[') and line.endswith(']')]

        for driver in odbc_drivers:
            if str(pattern).lower() in str(driver).lower():
                return driver

        assert False, "driver for '%s' not found" % pattern

    if database_type == DatabaseType.MSSQL:
        import pyodbc

        connection_string = 'Driver={%s};Server=%s,%s;Database=%s;Uid=%s;Pwd=%s'
        connection_string %= (
            get_driver_like('sql server'), connection_info.hostname, connection_info.port,
            connection_info.database, connection_info.login, connection_info.password,
        )
        pyodbc.pooling = False
        connection = pyodbc.connect(connection_string, autocommit=True)

        return connection

    if database_type == DatabaseType.ORACLE:
        import cx_Oracle

        connection_string = '%s/%s@%s:%s/%s'
        connection_string %= (
            connection_info.login, connection_info.password,
            connection_info.hostname, connection_info.port,
            connection_info.database,
        )

        connection = cx_Oracle.connect(connection_string)

        return connection

    if database_type == DatabaseType.POSTGRESQL:
        import pyodbc

        connection_string = 'DRIVER={%s};SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s'
        connection_string %= (
            get_driver_like('postgres'), connection_info.hostname, connection_info.port,
            connection_info.database, connection_info.login, connection_info.password,
        )

        connection = pyodbc.connect(connection_string, autocommit=True)

        return connection

    if database_type == DatabaseType.MYSQL:
        import mysql.connector

        connection = mysql.connector.connect(
            user=connection_info.login, password=connection_info.password,
            host=connection_info.hostname, port=connection_info.port,
            database=connection_info.database, connection_timeout=7200
        )

        return connection

def connection_task(database_type, connection_info: ConnectionInfo, connection_id,
                    query, queries_count, pause_time):
    connection = open_connection(database_type, connection_info)

    for query_id in range(queries_count):
        current_query = query.format(connection_id=connection_id, query_id=query_id)

        cursor = connection.cursor()
        cursor.execute(current_query)
        print(current_query)
        sys.stdout.flush()
        cursor.fetchall()
        cursor.close()

        time.sleep(pause_time)

    connection.close()

class TrailingTest(object):
    def __init__(self):
        self.database_type = None
        self.connection_info = None
        self.queue = list()

        self._task_connection_id = 0

    def add_task(self, query, queries_count, pause_time):
        self.queue.append(
            (self._task_connection_id, query, queries_count, pause_time)
        )
        self._task_connection_id += 1

    def run(self, threads_count):
        def thread_task(test: TrailingTest):
            try:
                while 1 == 1:
                    try:
                        connection_id, query, queries_count, pause_time = self.queue.pop(0)
                    except IndexError:
                        return  # queue is empty, all tasks done

                    connection_task(
                        test.database_type, test.connection_info, connection_id, query, queries_count, pause_time
                    )
            except Exception as ex:
                print("ERROR:")
                sys.stdout.flush()
                with contextlib.suppress(Exception):
                    print(ex)
                    sys.stdout.flush()

        threads = [
            threading.Thread(target=thread_task, args=(self,)) for _ in range(threads_count)
        ]
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

def main():
    trailing_test = TrailingTest()
    trailing_test.database_type = g_database_type
    trailing_test.connection_info = ConnectionInfo()
    trailing_test.connection_info.hostname = g_hostname
    trailing_test.connection_info.port = g_port
    trailing_test.connection_info.database = g_database
    trailing_test.connection_info.login = g_login
    trailing_test.connection_info.password = g_password

    prepare_connection = open_connection(trailing_test.database_type, trailing_test.connection_info)
    with contextlib.suppress(Exception):
        prepare_connection.cursor().execute("drop table test_trailing")
    prepare_connection.cursor().execute("create table test_trailing (c_id int, c_data varchar(32))")
    prepare_connection.close()

    connections_count = g_connections_count
    queries_per_connection = g_queries_count
    pause_time = g_pause_time

    for _ in range(connections_count):
        trailing_test.add_task(
            "select * from test_trailing where c_data = 'connection #{connection_id} - query #{query_id}'",
            queries_per_connection, pause_time
        )

    threads_count = g_threads_count

    trailing_test.run(threads_count)

if __name__ == "__main__":
    main()
