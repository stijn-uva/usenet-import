"""
For all files, find the block of headers with the "From:" header in it, and remove all data
occurring before that block of headers.
"""
import glob
import sys
import os


def clean(path):
    """
    Cleans a file

    Removes all data before the first block of text containing "From: ". Blocks of text
    are separated by two newline characters (`\n\n`).

    :param path:  Path to document
    :return string:  Cleaned document contents
    """
    with open(path) as docfile:
        document = docfile.read()
        docfile.close()

    if not document:
        pass

    from_pos = document.find("From: ")
    position = 0
    for position in reversed(range(0, from_pos - 1)):
        bit = document[from_pos - 2:from_pos]
        if bit == "\n\n":
            break

    if position > 0:
        document = document[position + 2:]

    return document


if len(sys.argv) < 2:
    print("Usage: clean.py <path to file or folder>")
    sys.exit(1)

if os.path.isfile(sys.argv[1]) and not os.path.isdir(sys.argv[1]):
    glob_path = sys.argv[1]
else:
    glob_path = sys.argv[1] + "/**"

# clean files
files_cleaned = 0

for filename in glob.iglob(glob_path, recursive=True):
    if not os.path.isdir(filename) and os.path.isfile(filename):
        cleaned = clean(filename)
        with open(filename, "w") as file:
            file.write(cleaned)
            file.close()
            files_cleaned += 1

print("Files cleaned: %i" % files_cleaned)
