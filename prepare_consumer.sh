#!/bin/bash
wget https://github.com/mozilla/geckodriver/releases/download/v0.21.0/geckodriver-v0.21.0-linux64.tar.gz
tar -xzf geckodriver-v0.21.0-linux64.tar.gz
chmod +x geckodriver
sudo mv geckodriver /usr/local/bin
rm geckodriver-v0.21.0-linux64.tar.gz

sudo apt -y update
sudo apt -y upgrade
sudo apt install -y python3-pip firefox

pip3 install --trusted-host pypi.python.org -r requirements.txt

chmod +x ~/my_script.sh
chmod 0600 ~/config.ini
crontab -l | { cat; echo "@reboot ~/my_script.sh"; } | crontab -