import dateutil.parser
import sqlite3
import time
import json
import re


class UsenetMboxParser:
    """
    Imports a Usenet archive into an SQLite database

    Assumes the SQLite database already exists (see import.py). Compatible with single-post A News archives and single-
    or multi-post Mbox archives (e.g. the UTZOO and Usenet Historical Archive archives).

    Also uses a crude (to be improved!) spam check to filter out obvious spam posts.

    Usage:
        parser = UsenetMboxParser()
        parser.open("alt.fan.warlord.mbox")
        parser.process_all()
    """
    opened_file = None
    path = ""
    buffer = ""
    offset = 0
    parsed = 0
    total = 0
    dictionary = {}
    timezones = {}

    def __init__(self, dictionary="dictionary.json", timezones="timezones"):
        """
        Load timezones and dictionary for timestamp parsing and spam filtering

        :param string dictionary: Path to a JSON file with a {"word":1}-style
                                  index of words
        :param string timezones:  Path to a text file with a "-8 PST PT HNP PT
                                  AKDT"-style listing of timezones (one per
                                  line)
        """
        try:
            self.dictionary = json.load(open(dictionary, "r"))
        except (IOError, json.decoder.JSONDecodeError) as e:
            print("Could load spam check dictionary - messages will not be checked for spam!")

        try:
            for tz in open(timezones, "r"):
                description = str(tz).split(" ")
                for abbreviation in description[1:]:
                    self.timezones[abbreviation.strip()] = int(float(description[0]) * 3600)
        except (IOError, IndexError) as e:
            print("Could not load timezones - dates may not be parsed correctly!")

    def open(self, file):
        """
        Open file for parsing

        :param string file: Path to file or folder to proces.
        """
        self.opened_file = open(file, encoding="ISO-8859-1", newline="\n")
        self.opened_file.seek(0)
        self.path = file
        print("\nProcessing %s" % self.path)

    def process_all(self, cursor):
        """
        Determine archive type and import/link messages one by one

        :param sqlite3.Cursor cursor: An SQLite3 cursor to a database into
                                      which posts are to be imported.
        """
        if not self.opened_file:
            raise RuntimeError("Load a file for processing first with open()")

        # determine whether we're dealing with A news or the more modern mbox format
        first = self.opened_file.readline()
        self.opened_file.seek(0)
        parse_method = self.parse_one_anews if len(first) > 0 and first[0] == "A" else self.parse_one_mbox

        results = parse_method()
        # loop through messages one by one
        while results:
            try:
                timestamp = time.mktime(
                    dateutil.parser.parse(results['timestamp'].strip().replace('--', '-'),
                                          tzinfos=self.timezones).timetuple())
            except (TypeError, ValueError) as e:
                print("\nCouldn't parse timestamp %s: %s" % (results["timestamp"], str(e)))
                timestamp = 0

            fields = (results['msgid'], results['sender'], timestamp, results['subject'])
            data = (results['msgid'], results['message'], results['headers'])
            groups = results['groups'].replace(',', ' ').replace('  ', ' ').split(' ')

            try:
                cursor.execute(
                    "INSERT INTO posts (`msgid`, `from`, `timestamp`, `subject`) VALUES (?, ? , ?, ?)", fields)
                cursor.execute(
                    "INSERT INTO postsdata (`msgid`, `message`, `headers`) VALUES (?, ?, ?)", data)
                self.total += 1
            except sqlite3.IntegrityError:
                # message ID already exists
                print("\nFound duplicate message %s, updating groups" % results['msgid'])
                current_groups = cursor.execute("SELECT `group` FROM postsgroup WHERE msgid = ?",
                                                (results['msgid'],)).fetchall()

                # make sure we don't create redundant group links
                for row in current_groups:
                    if row[0] in groups:
                        groups.remove(row[0])

            for group in groups:
                if group != "":
                    cursor.execute("INSERT INTO postsgroup (`msgid`, `group`) VALUES (?, ?)",
                                   (results['msgid'], group.strip()))

            results = parse_method()  # next one

        # clean up
        self.opened_file = None
        self.parsed = 0
        self.offset = 0

    def parse_one_mbox(self):
        """
        Processes the first available message in the mbox file being read

        :return dict: Message data
        """
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

            if re.match(r"From (.+)", line) and gap > 1 and re.match(r"([^ ]+): ([^\n]*)\n", next):
                # message finished
                break

            self.offset += len(line)

            # at least two newlines are expected between messages - keep track of those
            if line.strip() == "":
                gap += 1
            else:
                gap = 0

            buffer += line

            if next == "":
                # this is not EOF quite yet, so break instead of return to process the last message
                break

        buffer = buffer.strip()
        lines = buffer.split("\n")

        # the following two shouldn't really happen
        if buffer == "":
            print("\nEmpty buffer, skipping")
            return self.parse_one_mbox()

        if buffer[0:5] != "From ":
            if self.parsed > 0:  # it's okay if the first message lacks a from header (as in some single-message mboxes)
                print("\nMessage lacks starting 'From' header, skipping")
                return self.parse_one_mbox()
        else:
            # this only happens if the message is just a From line, which indicates archive corruption
            try:
                lines.pop(0)  # Get rid of From line
                if len(lines) == 0:
                    raise IndexError
            except IndexError:
                print("\nMessage is empty apart from From header, skipping (check for corrupt archive?)")
                return self.parse_one_mbox()

        # extract headers
        headers = {}
        header_buffer = ""
        current_header = ""

        while True:
            try:
                line = lines.pop(0)
            except IndexError:
                break

            if line.strip() == "":
                break

            header_buffer += line + "\n"
            header = re.match(r"([^ ]+): ([^\n]*)$", line)

            if not header:
                if current_header == "":
                    break  # not a header, apparently
                # multi-line, add to previous header (might be better to overwrite...?)
                headers[current_header] += " " + line
            else:
                current_header = header.group(1).lower()
                if current_header in headers:
                    headers[current_header] += " " + header.group(2)
                else:
                    headers[current_header] = header.group(2)

        # this can happen if the file isn't actually a mbox file
        if len(headers) == 0:
            print("\nMessage in archive %s lacks any headers, skipping file" % self.path)
            return False  # too many unknowns to make another attempt

        # this is the case for some really old messages
        if "message-id" not in headers and "article-i.d." in headers:
            headers["message-id"] = headers["article-i.d."]

        if "date" not in headers and "posted" in headers:
            headers["date"] = headers["posted"]

        if "subject" not in headers and "title" in headers:
            headers["subject"] = headers["title"]

        # the actual message is whatever's left at this point
        message = line + "\n".join(lines)

        # check if message is english
        if self.dictionary != {} and self.is_spam(message):
            print("\nMessage %s is probably spam, skipping" % headers["message-id"])
            return self.parse_one_mbox()

        try:
            data = {"msgid": headers["message-id"],
                    "sender": headers["from"],
                    "timestamp": headers["date"],
                    "subject": headers["subject"],
                    "message": message,
                    "groups": headers["newsgroups"],
                    "headers": header_buffer}

            self.parsed += 1
            print("Parsed message %i (%i total)\r" % (self.parsed, self.total), end="")

            return data
        except KeyError as e:
            print("\nMissing header '" + str(e) + "', skipping")
            return self.parse_one_mbox()

    def parse_one_anews(self):
        """
        Processes single message, formatted as A News

        A News puts each post ("article") in its own file, so this is relatively
        simple

        :return dict: Message data
        """
        data = {}

        header = self.opened_file.readline()
        if header == "":
            return False  # file already processed, end parsing

        if header[0] != "A":
            print("\nTried to parse %s as A News article, but lacks magic first byte" % self.path)
            return False

        data["msgid"] = header[1:]
        data["groups"] = self.opened_file.readline()
        data["sender"] = self.opened_file.readline()  # note: will be in bang path format!
        data["timestamp"] = self.opened_file.readline()
        data["subject"] = self.opened_file.readline()
        data["headers"] = "A" + "\n".join(data)
        data["message"] = ""

        while True:
            line = self.opened_file.readline()
            if line == "":
                break

            data["message"] += line + "\n"

        data["message"] = data["message"].rstrip()  # get rid of trailing newlines

        return data

    def is_spam(self, message):
        """
        Check if a message is spam

        Currently just checks if at least 10% of it is recognizably English.

        :param string message: Message to be checked, as a string.
        :return bool: Whether the message is spam
        """
        tokens = re.sub(r"[^a-z0-9 ]", " ", message.lower())
        tokens = re.sub(r"\s+", " ", tokens)
        tokens = tokens.split(" ")
        english_words = 0
        threshold = max(1, int(round(len(tokens) / 10)))

        while len(tokens) > 0:
            word = tokens.pop(0).strip().lower()
            if word in self.dictionary:
                english_words += 1
            if english_words >= threshold:
                break

        return english_words < threshold
