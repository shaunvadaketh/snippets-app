import logging
import argparse
import psycopg2

logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect(database="snippets")
logging.debug("Database connection established.")
# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)



def put(name, snippet):
    """
    Store a snippet with an associated name.

    Returns the name and the snippet
    """
    logging.info("Storing snippet {}: {}".format(name, snippet))
    with connection, connection.cursor() as cursor:
        
        try:
            command = "insert into snippets values (%s, %s)"
            cursor.execute(command, (name, snippet))
        except psycopg2.IntegrityError as e:
            connection.rollback()
            command = "update snippets set message=%s where keyword=%s"
            cursor.execute(command, (snippet, name))
    
    logging.debug("Snippet stored successfully.")
    return name, snippet

def get(name):
    """Retrieve the snippet with a given name.

    If there is no such snippet, return '404: Snippet Not Found'.

    Returns the snippet."""
    logging.info("Retreiving snippet {!r}".format(name))
    with connection, connection.cursor() as cursor:
        cursor.execute("select message from snippets where keyword=%s", (name,))
        row = cursor.fetchone()
        if not row:
         #No snippet was found with that name.
            return "404: Snippet Not Found"
        

        return row[0]
    
def catalog():
    #List of snippet-names that can be used to search for snippets in the get function
    
    logging.info("Listing snippets")
    with connection, connection.cursor() as cursor:
        cursor.execute("select keyword from snippets order by keyword")
        rows = cursor.fetchall()
        return rows

def search(name):
   #searches for snippets where messages contain inputted string
   logging.info("Searching for snippet")
   with connection, connection.cursor() as cursor:
       cursor.execute("select * from snippets where message like '%{}%'".format(name, ))
       rows = cursor.fetchall()
       if not rows:
           return "404: string not found in any message"
   return rows

def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Subparser for the put command
    logging.debug("Constructing put subparser")
    put_parser = subparsers.add_parser("put", help="Store a snippet")
    put_parser.add_argument("name", help="Name of the snippet")
    put_parser.add_argument("snippet", help="Snippet text")
    
    # Subparser for the get command
    logging.debug("Constructing get subparser")
    get_parser = subparsers.add_parser("get", help = "Retreive a snippet")
    get_parser.add_argument("name", help = "Name of the snippet")
    
    #Subparser for the catalog command
    logging.debug("Constructing catalog subparser")
    catalog_parser = subparsers.add_parser("catalog", help = "List of keywords used to get a snippet")
    
    #Subparser for search command
    logging.debug("Searching for string in message")
    search_parser = subparsers.add_parser("search", help = "searches for string in message")
    search_parser.add_argument("name", help = "string you want to search for")
    
    
    arguments = parser.parse_args()
    
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
        cat = catalog()
        print("List of snippet-names: {!r} ".format(cat))
    elif command == "search":
        sea = search(**arguments)
        justkeyword = [key[0] for key in sea]
        print ("The keyword of the snippet whose message contains the inputted string is: {}".format(justkeyword))
        
if __name__ == "__main__":
    main()
