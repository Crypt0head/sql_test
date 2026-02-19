#!/bin/bash

cd ~/Downloads

wget https://github.com/Crypt0head/sql_test/raw/refs/heads/main/libopenh264-2.5.1-linux64.7.so.bz2

sudo cp /etc/hosts /etc/hosts.old

echo "127.0.0.1       ciscobinary.openh264.org" | sudo tee -a /etc/hosts

sudo python3 -m http.server 80