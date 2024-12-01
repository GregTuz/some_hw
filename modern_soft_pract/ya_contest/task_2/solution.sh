#!/bin/bash

SORT_METHOD=$(tail -n 1 input.txt)

data_file=$(mktemp)
sed '1d;$d' input.txt > $data_file

format_output() {
    awk '{ printf("%s %s %d.%d.%d\n", $1, $2, $3, $4, $5) }'
}

if [ "$SORT_METHOD" = "date" ]; then
    LC_ALL=C sort -k5,5n -k4,4n -k3,3n -k2,2 -k1,1 $data_file | format_output > output.txt
elif [ "$SORT_METHOD" = "name" ]; then
    LC_ALL=C sort -k2,2 -k1,1 -k5,5n -k4,4n -k3,3n $data_file | format_output > output.txt
else
    echo "Unknown sorting method: $SORT_METHOD"
    rm "$DATA_FILE"
    exit 2
fi

rm $data_file
