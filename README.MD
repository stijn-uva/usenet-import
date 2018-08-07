# Usenet archive importer

## parser.py

Usage:

`python3 parser.py mbox-file [database-file]`

`python3 parser.py alt.fan.warlord.mbox import.db`

Parses the `mbox` file supplied and puts messages contained 
therein into a SQLite database. The SQLite database has two
tables:

### posts

| Field | Purpose |
|-------|---------------------|
| `msgid` | Message ID (unique) |
| `from` | Content of `From: ` header |
| `timestamp` | Content of `Date: ` header |
| `subject` | Mail subject |
| `message` | Full message, minus headers and surrounding whitespace |
| `headers` | All headers

### postsgroup
| Field | Purpose |
|-------|---------|
| `msgid` | Message ID (unique, matches message in `posts`) |
| `group` | Group name to which message was posted |

All messages have one single entry in `posts`, but may have
multiple entries in `postsgroup`.  