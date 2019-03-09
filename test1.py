#!/usr/bin/python

import psycopg2
#import ConfigParser
#from config import config
from configparser import ConfigParser

def config(filename='database.ini', section='redshift'):

    parser = ConfigParser()
    parser.read(filename)
 
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
 
    return db


def connect():
    """ Connect to the database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        print(params)
          
        print('Connecting to the database...')
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

    i = 0
    while i < 5000000: 
        # f-string would be nice but gets confused on ().
        #print(f"INSERT INTO bob (id, fname, lname) VALUES ({i},a{i},b{i})")
        try:
            cur.execute("INSERT INTO bob (id, fname, lname) VALUES ({},\'a{}\',\'b{}\')".format(i, i, i))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        i += 1

    conn.commit()

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
 
