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

The importer is also compatible with most of the messages found
in the Shofar FTP archives, provided they are run through 
`clean.py` (see below) first. Generally speaking, most archives 
should be parseable after cleaning.
 
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

## Other scripts
`clean.py` accepts a path to a file or directory as an argument.
All files are then "cleaned": that is, all data occurring before
the first block of text that contains the string `From: ` (with 
a space after `:`) is removed from the file.

## Things to keep in mind
The importer does its best to import archived messages too. To
this end, it will import messages with empty `Message-ID`s, as
well as messages lacking a `Newsgroups: ` reader as long as
they have an `Xrefs: ` header referencing at least one group.

Empty Message-IDs are replaced with a placeholder Message-ID of
the format `<index@imported-timestamp>` where `index` is an
incrementing number signifying the amount of ID-less messages
imported so far and `timestamp` is the UNIX timestamp at which
the import started; e.g. `<25@imported-1514764800>`. This only
applies to MBox-style posts: if the Message-ID is missing in an
A News archive, the message is ignored. 

This may lead to duplicates if the original message is also 
included in another imported archive; the importer will not 
notice the duplicate as it has a different Message-ID.