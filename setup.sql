CREATE TABLE IF NOT EXISTS `posts` (
  `msgid`      TEXT UNIQUE,
  `from`       TEXT,
  `timestamp`  INTEGER,
  `subject`    TEXT,
  PRIMARY KEY(`msgid`)
);

CREATE TABLE IF NOT EXISTS `postsgroup` (
  `msgid` TEXT,
  `group` TEXT
);

CREATE TABLE IF NOT EXISTS `postsdata` (
  `msgid`   TEXT,
  `message` TEXT,
  `headers` TEXT,
  PRIMARY KEY(`msgid`)
);
