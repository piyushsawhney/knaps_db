from configparser import ConfigParser

import psycopg2
from psycopg2.extras import execute_values


def config(filename='config/desktop.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


try:
    # read connection parameters
    params = config()
    # connect to the PostgreSQL server
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)


def run_upsert(statement, inputs):
    print(cur.mogrify(statement, (inputs)))
    cur.execute(statement, (inputs))
    conn.commit()


def perform_upsert(table_name, columns, values, conflict_column):
    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT ({conflict_column}) DO UPDATE SET
        {', '.join(f"{col} = EXCLUDED.{col}" for col in columns if col != conflict_column)}
        """

    # values = [tuple(x) for x in df.to_numpy()]
    execute_values(cur, insert_query, values)
    conn.commit()
