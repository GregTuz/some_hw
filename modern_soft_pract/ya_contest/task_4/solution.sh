#!/bin/bash

# Чтение строк из input.txt
while IFS= read -r url; do
    # Заменяем все вхождения 'google' на 'yandex'
    url=$(echo "$url" | sed 's/google/yandex/g')

    # Инициализация переменных
    scheme=""
    host=""
    port=""
    args=""

    # Парсим схему (http:// или https://)
    if [[ "$url" =~ ^(https?://) ]]; then
        scheme="${BASH_REMATCH[1]}"
        url="${url#${scheme}}"  # Убираем схему
    fi

    # Парсим хост и порт (если есть)
    if [[ "$url" =~ ^([^:/]+)(:([0-9]+))?(/.*)?$ ]]; then
        host="${BASH_REMATCH[1]}"
        port="${BASH_REMATCH[3]}"
        # Если есть параметры, сохраняем их
        if [[ -n "${BASH_REMATCH[4]}" ]]; then
            args="${BASH_REMATCH[4]:1}"  # Убираем символ '?'
        fi
    fi

    # Выводим результат в нужном формате
    if [[ -n "$scheme" ]]; then
        echo "Scheme: $scheme"
    fi

    if [[ -n "$host" ]]; then
        echo "Host: $host"
    fi

    if [[ -n "$port" ]]; then
        echo "Port: $port"
    fi

    if [[ -n "$args" ]]; then
        echo "Args:"
        # Парсим аргументы
        while IFS='&' read -r pair; do
            key=$(echo "$pair" | cut -d '=' -f 1)
            value=$(echo "$pair" | cut -d '=' -f 2)
            echo "  Key: $key; Value: $value"
        done <<< "$args"
    fi

    # Печатаем пустую строку после каждого URL
    echo

done < input.txt

