#!/bin/bash

CSV_PATH=$1

user="root"
password="q123"
database="history_db"

mysql -u"$user" -p"$password" "$database" <<EOF
-- LOAD DATA INFILE '/var/lib/mysql-files/csv.csv'
LOAD DATA INFILE '${CSV_PATH}'
INTO TABLE history_db.tmp_cmd_usage
CHARACTER SET latin1
FIELDS TERMINATED BY ',' ESCAPED BY ''
LINES TERMINATED BY '\n';
EOF
