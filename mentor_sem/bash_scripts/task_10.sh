dir_sorting=$1
logger_file=$2

sort(){
	dir=$1
	shift
	ext=($@)
	
	for file in $(find "$dir_sorting" -maxdepth 1 -type f \( -name "*.$ext" \)); do
        mv -v "$file" "$dir" >> "$logger_file" 2>&1
    done
}

mkdir -p image_dir doc_dir

sort image_dir jpg png gif

sort doc_dir txt pdf docx

echo "Сортировка файлов завершена: $(date)" >> "$logger_file"

#После чего для того чтобы поставить скрипт на автозапуск вызываем команду 'crontab -e' и добавляем в конец файла '0 0 * * * ~./task_10.sh'
