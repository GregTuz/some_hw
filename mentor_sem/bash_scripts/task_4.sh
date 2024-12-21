hi(){
name=$1
echo "Hello, $name"
}

add(){
a=$1
b=$2
echo "Summ is equal $(($a + $b))"
}


echo "Введите имя"
read -r name
hi $name

echo "Введите первое число"
read -r a

echo "Введите второе число"
read -r b

add $a $b
