#! /usr/bin/bash
cd /home/gembase/dashboard-server/nginx
sudo cp -r nginx.conf /etc/nginx/nginx.conf
sudo service nginx restart
