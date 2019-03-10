#!/usr/bin/python
"""
    test program to DELETE, INSERT and SELECT
    from an AWS Redshift database.
"""
from configparser import ConfigParser
import psycopg2

def config(filename='database.ini', section='redshift'):
    """ Parses .ini file, returns a map of the contents.
    """

    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        # convert from list to map.
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('{0} does not have a {1} section.'.format(filename, section))

    return db

def connect():
    """
        Connect to the database server
    """

    try:
        params = config()

        local_conn = psycopg2.connect(**params)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None

    return local_conn

def do_insert(conn, should_commit, tuples):
    """
        Create a multi-value insert statement and execute it.

        :param conn - database connection object
        :param should_commit - boolean to commit/not commit after executing the INSERT
        :param tuples - list of tuples containing the data elements.

        :returns boolean indicating success or failure.
    """

    part = ""
    for tup in tuples:
        part += "({},{},{}),".format(*tup)

    part = part[:-1]        # chop last character (the trailing comma)
    insert_statement = "INSERT INTO bob (id, fname, lname) VALUES " + part

    try:
        cur = conn.cursor()
        cur.execute(insert_statement)
        if should_commit:
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False

    return True


if __name__ == '__main__':

    CONN = connect()
    CUR = CONN.cursor()

    CUR.execute('DELETE FROM bob')
    CONN.commit()

    i = 1
    TUPLES = []
    while i <= 22277:
        if i % 100 == 0:
            if not do_insert(CONN, True, TUPLES):
                print("do_insert() failed.")
                exit(27)
            TUPLES = []

        TUPLES.append((i, i, i))
        i += 1

    # check if there are leftover tuples to INSERT
    if TUPLES:
        if not do_insert(CONN, True, TUPLES):
            print("do_insert() failed.")
            exit(28)

    CUR.execute('SELECT * FROM bob ORDER BY id DESC')
    while True:
        REC = CUR.fetchone()
        if REC is None:
            break
        #print(REC)

    CUR.close()
    CONN.close()
