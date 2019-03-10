#!/usr/bin/python

import sys
import psycopg2
from configparser import ConfigParser

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
    """ Connect to the database server 
    """
    conn = None

    try:
        params = config()
          
        conn = psycopg2.connect(**params)
 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None

    finally:
        return conn

def do_insert(conn, autocomplete, tuples): 
   
    part = ""
    for tup in tuples:
        part += "({},{},{}),".format(*tup)

    part = part[:-1]        # chop last character (the comma)
    insert_statement = "INSERT INTO bob (id, fname, lname) VALUES " + part

    try:
        cur.execute(insert_statement) 
        if autocomplete: 
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False

    return True


if __name__ == '__main__':

    conn = connect()
    cur = conn.cursor()

    cur.execute('DELETE FROM bob')
    conn.commit()

    i = 1
    tuples = []
    while i <= 22277: 
        if i % 100 == 0:
            if not do_insert(conn, True, tuples):
                print("do_insert() failed.")
                sys.exit(27)
            tuples = []

        tuples.append((i, i, i));
        i += 1

    # check if there are leftover tuples to INSERT
    if len(tuples) > 0:
        do_insert(conn, True, tuples);

    # execute a statement
    cur.execute('SELECT version()')
 
    # display the database server version
    print(cur.fetchone())

    cur.execute('SELECT * FROM bob ORDER BY id DESC')
    while True:
        rec = cur.fetchone()
        if rec is None:
            break
        #print(rec)

    conn.close()
 
