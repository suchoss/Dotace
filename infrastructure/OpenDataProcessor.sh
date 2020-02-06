#!/bin/bash

# ref: https://askubuntu.com/a/30157/8698
if ! [ $(id -u) = 0 ]; then
   echo "The script need to be run as root." >&2
   exit 1
fi

if [ $SUDO_USER ]; then
    real_user=$SUDO_USER
else
    real_user=$(whoami)
fi

# Commands that you don't want running as root would be invoked
# with: sudo -u $real_user
# So they will be run as the user who invoked the sudo command
# Keep in mind if the user is using a root shell (they're logged in as root),
# then $real_user is actually root
# sudo -u $real_user non-root-command

# Commands that need to be ran with root would be invoked without sudo
# root-command

#### lets go ####

apt update

# install all packages
apt-get -y install docker.io git postgresql-client python3-pip
# add alias
#echo "alias docker='sudo docker'" > ~/.bash_aliases
#source ~/.bash_aliases

# run postgresql
mkdir -p /var/docker/postgre/data
docker run --shm-size=1g --name mypostgres -e POSTGRES_PASSWORD=xxx -v /var/docker/postgre/data:/var/lib/postgresql/data -p 5432:5432 -d postgres:12.1

# init cedr db
pushd .
cd /opt
sudo -u $real_usergit clone https://github.com/kokes/od.git
cd /opt/od/data/cedr
psql -h localhost -p 5432 -U postgres -f init.sql

# install requirements
pip3 install -r /opt/od/requirements.txt

# run python on cedr
python3 parse.py

# copy data to db
sudo -u $real_usergit eval "$(sed 's/psql/psql -h localhost -p 5432 -U postgres/g' copy.sh)"