#!/bin/bash
getfattr -n $1 --absolute-names $2 2>/dev/null | while read line
do
    if [[ $line == \#* ]]
    then
        echo -n $(echo $line | cut -d ' ' -f 3-)
        echo -n ' '
    fi
    if [[ $line == ceph* ]]
    then
        size=$(echo $line | cut -d = -f 2 | tr -d '"')
    echo $size
    fi
done | sort -k2n | while read line
do
  name=$(echo $line | cut -d ' ' -f 1)
  size=$(echo $line | cut -d ' ' -f 2)
  echo $name $size
done
