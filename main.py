from configparser import ConfigParser
import psycopg2
import os

# VARS
QUERY = '''
SELECT r.rolname as username,r1.rolname as "role"
FROM pg_catalog.pg_roles r JOIN pg_catalog.pg_auth_members m
ON (m.member = r.oid)
JOIN pg_roles r1 ON (m.roleid=r1.oid)
where r1.rolname != 'ldap_users'                             
ORDER BY 1;
'''
OUTPUT = 'output.csv'

def config(section, filename='database.ini'):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    print(db)
    return db


def querydb(section, query):
    conn = None
    try:
        params = config(section)
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        return error
    finally:
        if conn is not None:
            conn.close()
    return result

def parse(result):
    with open(OUTPUT, 'w') as f:
        f.write('Users; Groups;\n')
        for line in result:
            if line == result[0]:
                curruser = line[0]
                f.write(f'{line[0]}; {line[1]}')
                continue
            if curruser == line[0]:
                f.write(f', {line[1]}')
            else:
                curruser = line[0]
                f.write(';\n')
                f.write(f'{line[0]}; {line[1]}')
            if line ==  result[-1]:
                f.write(';\n')


if __name__ == '__main__':
    result = querydb('postgresql', QUERY)
    os.remove(OUTPUT)
    parse(result)
    exit(0)
