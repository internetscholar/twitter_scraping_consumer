#!/bin/bash
#wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
#sudo sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list'
#sudo apt-get -y update
#sudo apt-get -y install google-chrome-stable unzip
#wget https://chromedriver.storage.googleapis.com/2.37/chromedriver_linux64.zip
#unzip chromedriver_linux64.zip
#rm chromedriver_linux64.zip
#sudo mv chromedriver /usr/local/bin
sudo apt-get -y update
sudo apt-get -y install python3-pip fontconfig
pip3 install --trusted-host pypi.python.org -r requirements.txt
chmod 0600 ~/config.ini
chmod +x ~/my_script.sh
crontab -l | { cat; echo "@reboot ~/my_script.sh"; } | crontab -
wget https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-2.1.1-linux-x86_64.tar.bz2
tar -vxjf phantomjs-2.1.1-linux-x86_64.tar.bz2
rm phantomjs-2.1.1-linux-x86_64.tar.bz2
sudo mv phantomjs-2.1.1-linux-x86_64/bin/phantomjs /usr/local/bin
rm -R phantomjs-2.1.1-linux-x86_64/