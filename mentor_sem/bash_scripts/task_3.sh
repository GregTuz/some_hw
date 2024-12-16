echo "Enter any number"
read -r num
j=0

if [[ $num > 0 ]]; then
    echo "$num is > 0"
    while [[ $j -le $num ]]; do
        echo $j
        ((j++))
    done
elif [[ $num = 0 ]]; then
    echo "$num is = 0 and j is already equal 0"
else
    echo "$num is < 0"
    while [[ $j -gt $num ]]; do
        echo $j
        ((j--))
    done
fi


