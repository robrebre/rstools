#!/usr/bin/python

from psycopg2.extras import DictCursor
import pandas as pd

'''
Methods to help facilitate modifying/creating/dropping objects with view dependencies,
meant for Amazon Redshift databases (though this will most likely work in most postgres DBs)

The following set of methods are meant to be used in the following kind of workflow:
* Recursively Calculate all view dependencies of an object
  * Note - redshift does not have recursive CTEs, so this has to be looped in the script
* Drop the views, execute a piece of SQL to modify/alter the object, and recreate the views
  * This portion can be done in a single transaction to guarantee all views are recreated, or in
    serial transactions to continue past views that error on recreation 

Methods:
* view_deps
  * Returns an iterable of view dependencies, containing schema name, view name, owner, definition

* drop_execute_recreate
  * Drops all view dependencies of a given object, executes a SQL statement, and recreates all view dependencies


Example pseudo-code usage:
The following code replaces a view with a table - it will drop all dependent views, drop the original object,
execute the SQL, and recreate the dependent views

```
conn = <some psycopg2 connection>
sql = """
  DROP VIEW test_schema.test_summary;
  CREATE TABLE test_schema.test_summary AS SELECT ...
"""
drop_execute_recreate(conn, table='test_schema.test_summary', sql=sql)
```

'''

def view_deps(conn, table, maxdepth=10):
  schema = table.split('.')[0]
  unqual_table = table.split('.')[1]

  depth = 0
  curs = conn.cursor(cursor_factory=DictCursor)

  def recurse(curs, schema, unqual_table, depth,  maxdepth=maxdepth):
    ''' 
      The following base query retrieves all dependent view schema, name, owner, and definitions 
        
      NOTE - this expects a dict cursor
      NOTE 2 - this base query does not work w/ views in public schema, and needs to be adjusted if required 
    '''
    if depth+1 < maxdepth: 
      base_query = """
       SELECT DISTINCT 
        pgv.schemaname,
        pgv.viewname,
        pgv.viewowner,
        pgv.definition,
        {depth} AS depth
       FROM pg_class c_p
          INNER JOIN pg_depend d_p 
            ON c_p.relfilenode = d_p.refobjid
          INNER JOIN pg_depend d_c 
            ON d_p.objid = d_c.objid
          INNER JOIN pg_class c_c 
            ON d_c.refobjid = c_c.relfilenode
          INNER JOIN pg_namespace n_p 
            ON c_p.relnamespace = n_p.oid
          INNER JOIN pg_namespace n_c 
            ON c_c.relnamespace = n_c.oid
          INNER JOIN pg_views pgv
            ON pgv.schemaname = n_c.nspname
            AND pgv.viewname = c_c.relname  
        WHERE d_c.deptype = 'i'::"char" 
            AND c_c.relkind = 'v'::"char"
            AND lower(n_p.nspname) = lower('{schema}')
            AND lower(c_p.relname) = lower('{unqual_table}')
            AND NOT (lower(pgv.schemaname) = lower('{schema}') AND lower(pgv.viewname) = lower('{unqual_table}') )
        """.format(schema=schema,
                    unqual_table=unqual_table,
                    depth=depth)

      curs.execute(base_query)
      for row in curs.fetchall():
        # TODO - check for circular dependencies 
        yield list(row)
        print row['viewname']
        for a in recurse(curs, row['schemaname'], row['viewname'], depth+1):
          yield list(a)

    else:
      # max depth exceeded, eject!
      yield

  return recurse( curs, schema, unqual_table, 0 )  


def drop_execute_recreate(conn, table, sql):
  """
    Takes a tablename, and calculates any view dependencies recursively
    Then drops all dependencies, and executes the specified SQL before
      recreating the dependencies

    Note that the autocommit setting on conn will influence if this all happens
      atomically, or as serial transactions

    TODO - probably need to add in additinoal sql to execute post-recreation, for things like
      permissions/usage 
  """
  cursor = conn.cursor()

  # Load into dataframe so we can easily sort/dedupe
  df = pd.DataFrame(  list(view_deps(conn, table))  )
  df.columns= ['schema', 'name', 'owner', 'definition', 'depth']

  # Sort by depth desc, and drop duplicates, keeping the last instance (lowest depth) 
  df.sort_values('depth', ascending=False, inplace=True)
  df.drop_duplicates( subset=['schema', 'name', 'owner', 'definition'], keep='first', inplace=True) 

  # Drop all dependent views - could use cascade on base view, but this is safer
  for a in range(0, len(df)):
    query = "DROP VIEW %s.%s;" % ( df.iloc[a]['schema'], df.iloc[a]['name'] ) 
    cursor.execute(query)

  # Execute sql
  cursor.execute(sql) 

  # Recreate views - must start from lowest depth now - make sure to reset owner back to original
  df.sort_values('depth', ascending=True, inplace=True)
  for a in range(0, len(df)):
    query = """
      CREATE VIEW {schema}.{name} AS
       {definition} 
    """.format(schema=df.iloc[a]['schema'],
                name=df.iloc[a]['name'],
                definition=df.iloc[a]['definition'])
    print query
    cursor.execute(query)

    query = """
      ALTER TABLE {schema}.{name} OWNER TO {owner}
    """.format(schema=df.iloc[a]['schema'],
                name=df.iloc[a]['name'],
                owner=df.iloc[a]['owner'])
    print query
    cursor.execute(query)

  conn.commit()
