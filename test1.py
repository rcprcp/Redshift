#!/usr/bin/python

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
        print(params)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Filename {0} does not have a {1} section.'.format(filename, section))
 
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


if __name__ == '__main__':
    print("hello")
    conn = connect()
    cur = conn.cursor()

    cur.execute('delete from bob')
    conn.commit()

    i = 1
    tuples = []
    while i <= 50000: 
        if i % 100 == 0:
            part = ""
            for tup in tuples:
                part += "({},{},{}),".format(*tup)

            part = part[:-1]        #chop last character.
            insert = "INSERT INTO bob (id, fname, lname) VALUES " + part
            tuples = []
            
            try:
                cur.execute(insert) 
                conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)

        tuples.append((i, i, i));
        i += 1

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
 
