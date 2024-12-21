# Если файла для вывода не существует - создаем его
if [ !  -f file_extensions.txt ]
then
    echo "Created file - file_extensions.txt"
    touch file_extensions.txt
else
    > file_extensions.txt
    echo "File file_extensions.txt already exists"
fi

# Заполняем файл с расширениями
for item in *; do
    if [[ -f $item ]]; then
        extension=${item##*.}
        if [[ $extension != $item ]]; then
            echo "$item : $extension" >> file_extensions.txt
        else
            echo "$item : no_extension" >> file_extensions.txt
    	fi
    elif [[ -d $item ]]; then 
	echo "$item : directory" >> file_extensions.txt
    fi
done

# Проверка наличия файла
echo "Enter filename to check existanse"
read -r file
if [[ -f $file ]]; then
   echo "$file exists"
else
   echo "$file does not exist"
fi

# Циклический вывод файлов и их прав доступа
for file in *; do
    echo "$file - $(ls -ld "$file" | awk '{print $1}')"
done
