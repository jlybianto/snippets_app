import logging
import argparse
import sys
import psycopg2
import psycopg2.extras

# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)

logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect("dbname='snippets' user='action' host='localhost'")
logging.debug("Database connection established.")


def put(name, snippet):
  """
  Store a snippet with an associated name.
  Returns the name and the snippet.
  """
  logging.info("Storing snippet {!r}: {!r}".format(name, snippet))
  with connection, connection.cursor() as cursor:
    try:
      cursor.execute("insert into snippets values (%s, %s)", (name, snippet))
    except psycopg2.IntegrityError as e:
      connection.rollback()
      cursor.execute("update snippets set message=%s where keyword=%s", (name, snippet))
    logging.debug("Snippet stored successfully.")
  return name, snippet


def get(name):
  """
  Retrieve the snippet with a given name.
  If there is no such snippet raise an error.
  Returns the snippet.
  """
  logging.info("Retrieving snippet {!r}".format(name)) 
  with connection, connection.cursor() as cursor:
    cursor.execute("select message from snippets where keyword=%s", (name, ))
    row = cursor.fetchone()
  
  if not row:
    # No snippet was found with that name.
    logging.debug("Snippet {!r} does not exist.".format(name))
  else:
    logging.debug("Snippet retrieved successfully.")
    return row[0]


def catalog():
  """
  Inquire all names in the snippets table.
  """
  logging.info("Inquirying snippets database")
  with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
    cursor.execute("select keyword from snippets order by keyword")
    rows = cursor.fetchall()
    for row in rows:
      print row['keyword']
  logging.debug("Snippets database inquiry complete.")
  
  
def main():
  """
  Main function
  """
  logging.info("Constructing parser")
  parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")
  
  subparsers = parser.add_subparsers(dest="command", help="Available commands")
  
  # Subparser for the put command
  logging.debug("Constructing put subparser")
  put_parser = subparsers.add_parser("put", help="Store a snippet")
  put_parser.add_argument("name", help="The name of the snippet")
  put_parser.add_argument("snippet", help="The snippet text")
  
  # Subparser for the get command
  logging.debug("Constructing get subparser")
  get_parser = subparsers.add_parser("get", help="Retrieve a snippet")
  get_parser.add_argument("name", help="The name of the snippet")
  
  # Subparser for the catalog command
  logging.debug("Constructing catalog subparser")
  subparsers.add_parser("catalog", help="Inquire all available snippet keywords")
  
  arguments = parser.parse_args(sys.argv[1:])
  # Convert parsed arguments from Namespace to dictionary
  arguments = vars(arguments)
  command = arguments.pop("command")
  
  if command == "put":
    name, snippet = put(**arguments)
    print("Stored {!r} as {!r}".format(snippet, name))
  elif command == "get":
    snippet = get(**arguments)
    print("Retrieved snippet: {!r}".format(snippet))
  elif command == "catalog":
    print("Inquired snippet keywords:\n")
    catalog()
    
    
if __name__ == "__main__":
  main()