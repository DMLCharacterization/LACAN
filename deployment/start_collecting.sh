#!/bin/sh
("$1")&

while read ip; do
  (ssh "$ip" "$1")&
done < "$2"
