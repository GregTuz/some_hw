#!/bin/bash

lines=$(wc -l < input.txt)
words=$(wc -w < input.txt)
letters=$(grep -o '[a-zA-Z]' input.txt | wc -l)

echo "Input file contains:" > output.txt
echo "$letters letters" >> output.txt
echo "$words words" >> output.txt
echo "$lines lines" >> output.txt
