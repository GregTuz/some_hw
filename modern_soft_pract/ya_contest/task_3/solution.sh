#!/bin/bash

# Проверяем наличие входного файла
if [ ! -f input.txt ]; then
    echo "File input.txt not found!"
    exit 1
fi

index=0
result=0
coefficients=()
x=0
power=1

while IFS= read -r line; do
    if [ $index -eq 0 ]; then
        x=$line
    else
        coefficients+=("$line")
    fi
    index=$((index + 1))
done < input.txt

for coefficient in "${coefficients[@]}"; do
    term=0
    for ((i = 0; i < power; i++)); do
        term=$((term * x))
    done
    term=$((term + coefficient))
    result=$((result + term))
    power=$((power + 1))
done


echo "$result" > output.txt

