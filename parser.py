import sqlite3
import sys
import re


class UsenetMboxParser:
    """
    Parses a Usenet Mbox archive (such as found on e.g. the Usenet Historical Archive) into a pre-existing SQLite
    database.

    Usage:
        parser = UsenetMboxParser()
        parser.open("alt.fan.warlord.mbox")
        parser.parse()
    """
    opened_file = None
    path = ""
    buffer = ""
    offset = 0
    parsed = 0

    def parser(self):
        pass

    def open(self, file):
        try:
            self.opened_file = open(file, encoding="ISO-8859-1", newline="\n")
            self.opened_file.seek(0)
            self.path = file
        except IOError:
            raise UsenetMboxParserException("Tried to load file %s for parsing, but could not open for reading" % file)

    def parse(self, cursor):
        """ Open file for parsing and save messages one by one """
        if not self.opened_file:
            raise UsenetMboxParserException("No file currently loaded for parsing")

        results = self.process_one()
        # loop through messages one by one
        while results:
            fields = (results['msgid'],
                      results['sender'],
                      results['timestamp'],
                      results['subject'],
                      results['message'],
                      results['headers'])

            groups = results['groups'].replace(',', ' ').replace('  ', ' ').split(' ')

            try:
                cursor.execute(
                    "INSERT INTO posts (`msgid`, `from`, `timestamp`, `subject`, `message`, `headers`) VALUES (?, ? , ?, ?, ?, ?)",
                    fields)
            except sqlite3.IntegrityError:
                print("\nFound duplicate message %s, updating groups" % results['msgid'])
                current_groups = cursor.execute("SELECT group FROM postsgroup WHERE msgid = ?",
                                                (results['msgid'])).fetchall()
                # make sure we don't create redundant group links
                for row in current_groups:
                    if row[0][0] in groups:
                        groups.remove(row[0][0])

            for group in groups:
                cursor.execute("INSERT INTO postsgroup (`msgid`, `group`) VALUES (?, ?)",
                               (results['msgid'], group.strip()))

            results = self.process_one()  # next one

        # clean up
        self.opened_file = None
        self.group = None
        print("\nFinished importing archive %s! " % self.path)

    def process_one(self):
        """ Processes the first available message in the mbox file being read """
        self.opened_file.seek(self.offset)

        buffer = ""  # the message
        gap = 0

        # first capture one full message, and no more than that
        while True:
            line = self.opened_file.readline()

            position = self.opened_file.tell()
            next = self.opened_file.readline()
            self.opened_file.seek(position)  # pretend we didn't peek ahead

            if line == "":
                # EOF
                return False

            if re.match(r"From (.+)", line) and gap > 1 and re.match(r"([^ ]+): ([^\n]+)\n", next):
                # message finished
                break

            self.offset += len(line)

            # at least two newlines are expected between messages - keep track of those
            if line.strip() == "":
                gap += 1
            else:
                gap = 0

            buffer += line

            # this is not EOF quite yet, so break instead of return to process the last message
            if next == "":
                break

        buffer = buffer.strip()
        lines = buffer.split("\n")

        # the following two shouldn't really happen
        if buffer == "":
            print("\nEmpty buffer, skipping")
            return self.process_one()

        if buffer[0:5] != "From ":
            print("\nMessage lacks starting 'From' header, skipping")
            return self.process_one()

        # this only happens if the message is just a From line, which indicates archive corruption
        try:
            lines.pop(0)  # Get rid of From line
            if len(lines) == 0:
                raise IndexError
        except IndexError:
            print("\nMessage is empty apart from From header, skipping (check for corrupt archive?)")
            return self.process_one()

        # extract headers
        headers = {}
        header_buffer = ""
        current_header = ""

        for line in lines:
            if line.strip() == "":
                break

            header_buffer += line + "\n"
            header = re.match(r"([^ ]+): ([^\n]+)$", line)

            if not header:
                # multi-line, add to previous header (might be better to overwrite...?)
                headers[current_header] += " " + line
            else:
                current_header = header.group(1).lower()
                if current_header in headers:
                    headers[current_header] += " " + header.group(2)
                else:
                    headers[current_header] = header.group(2)

        # the actual message is whatever's left at this point
        message = line + "\n".join(lines)

        try:
            data = {"msgid": headers["message-id"],
                    "sender": headers["from"],
                    "timestamp": headers["date"],
                    "subject": headers["subject"],
                    "message": message,
                    "groups": headers["newsgroups"],
                    "headers": header_buffer}

            self.parsed += 1
            print("Parsed message %i\r" % self.parsed, end="")

            return data
        except KeyError as e:
            print("\nMissing header '" + str(e) + "', skipping")
            return self.process_one()


class UsenetMboxParserException(Exception):
    pass


# parse command line
if len(sys.argv) < 2:
    print("Usage:")
    print("  parser.py mbox-file [database-file]")
    print("")
    print("Example:")
    print("  parser.py alt.fan.warlord.mbox warlord.db")

# set up database connection
dbpath = sys.argv[2] if len(sys.argv) > 2 else "usenet-import.db"
database = sqlite3.connect(dbpath)
database.text_factory = str  # these are all pre-unicode
cursor = database.cursor()

cursor.execute(
    "CREATE TABLE IF NOT EXISTS `posts` ( `msgid` TEXT UNIQUE, `from` TEXT, `timestamp` INTEGER, `subject` TEXT, `message` TEXT, `headers` TEXT, PRIMARY KEY(`msgid`) )")
cursor.execute("CREATE TABLE IF NOT EXISTS `postsgroup` ( `msgid` TEXT, `group` TEXT )")

# run!
parser = UsenetMboxParser()
parser.open(sys.argv[1])
parser.parse(cursor)
database.commit()
