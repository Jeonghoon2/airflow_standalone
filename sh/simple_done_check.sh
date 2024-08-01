#!/bin/bash
FILE=$1

echo "_DONE Path = $FILE"

if [[ -e "$FILE" ]]; then
	figlet "Let's move on!"
	exit 0
else
    echo "I'll be back => $DONE_PATH_FILE"
	exit 1
fi
