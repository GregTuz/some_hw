dir=$1
logger=$2
dir_to_cp=$3
now=$(date +"%Y-%m-%d_%H:%M:%S")
count=0

for file in $dir/*; do
	((count++))
	cp -p $file $dir_to_cp/$(basename $file.$now)
	echo 'Скопирован файл $file' >> $logger
done

echo "Скопировано $count файлов" >> $logger

notify-send "Копирование завершено. Файлов скопировано - $count"
