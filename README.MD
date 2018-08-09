# Usenet archive importer

## import.py

Usage:

`python3 import.py posts-file [database-file]`

`python3 import.py alt.fan.warlord.mbox import.db`

Parses the `posts-file` supplied and inserts messages contained 
therein into a SQLite database (named `usenet-import.db` by
default). The file can be either a mbox file or an A News 
article. 

`posts-file` can also be a folder, in which case that folder 
will be crawled recursively and every file found within it 
or its subfolders will be parsed (so be careful with non-mbox 
files in there).

This makes the importer compatible with both the Usenet
Historical Archive and the UTZOO archive (though it is
recommended that you clean out non-mbox/anews files from
the latter before importing it).
 
The SQLite database has three tables:

### posts

This table contains post metadata and can be used to search 
for posts based on subject or author, or posts within a 
specific timeframe.

| Field | Purpose |
|-------|---------------------|
| `msgid` | Message ID (unique), based on `Message-ID` header |
| `from` | Content of `From: ` header |
| `timestamp` | Content of `Date: ` header, converted to a UNIX timestamp |
| `subject` | Post subject |

### postsgroup

All messages have one single entry in `posts`, but may have
multiple entries in `postsgroup`, for messages that were 
posted to multiple newsgroups.  

| Field | Purpose |
|-------|---------|
| `msgid` | Message ID (matches `msgid` in `posts`) |
| `group` | Group name to which message was posted |

### postsdata
This table contains the full message split up in message
body and headers respectively.

| Field | Purpose |
|-------|---------|
| `msgid` | Message ID (unique, matches `msgid` in `posts`) |
| `message` | Full message, minus headers and surrounding whitespace |
| `headers` | All headers