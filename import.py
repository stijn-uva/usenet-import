import parser
import sqlite3
import glob
import sys
import os

# parse command line
if len(sys.argv) < 2:
    print("Usage:")
    print("  import.py mbox-file|mbox-folder [database-file]")
    print("If mbox-folder is given, the folder will be parsed recursively (so be careful to make sure it and its sub-")
    print("folders contain only mbox files)")
    print("")
    print("Example:")
    print("  parser.py alt.fan.warlord.mbox warlord.db")
    sys.exit(1)

# set up database connection
dbpath = sys.argv[2] if len(sys.argv) > 2 else "usenet-import.db"
database = sqlite3.connect(dbpath)
database.text_factory = str  # these are all pre-unicode
cursor = database.cursor()

# run table setup
with open("setup.sql") as setup:
    print("Setting up database...")
    cursor.executescript(setup.read())

# load and run parser
parser = parser.UsenetMboxParser(dictionary="dictionary.json", timezones="timezones")
if os.path.isdir(sys.argv[1]):
    for filename in glob.iglob(sys.argv[1] + "/**", recursive=True):
        if not os.path.isdir(filename):
            parser.open(filename)
            parser.process_all(cursor)
            database.commit()
else:
    parser.open(sys.argv[1])
    parser.process_all(cursor)

database.commit()
