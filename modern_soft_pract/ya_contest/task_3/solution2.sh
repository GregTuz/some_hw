#!/bin/bash

# Проверка наличия входного файла
if [ ! -f input.txt ]; then
    echo "File input.txt not found!"
    exit 1
fi

# Чтение данных из файла
x=0
coefficients=()

# Чтение строк из файла
index=0
while IFS= read -r line || [ -n "$line" ]; do
    # Пропускаем пустые строки
    if [ -z "$line" ]; then
        continue
    fi

    if [ $index -eq 0 ]; then
        x=$line  # Сохраняем первое число как x
    else
        coefficients+=("$line")  # Добавляем остальные числа как коэффициенты
    fi
    index=$((index + 1))
done < input.txt

# Проверка на наличие коэффициентов
if [ ${#coefficients[@]} -eq 0 ]; then
    echo "No coefficients found in the file!"
    exit 1
fi

# Вычисление полинома с использованием метода Горнера
result=0
for ((i=${#coefficients[@]}-1; i>=0; i--)); do
    result=$((result * x + ${coefficients[$i]}))
done

# Вывод результата в файл
echo "$result" > output.txt

