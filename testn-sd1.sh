#!/bin/bash

[ ! "$(docker network ls | grep sd-net )" ] && \
	docker network create --driver=bridge --subnet=172.20.0.0/16 sd-net


if [  $# -le 1 ] 
then 
		echo "usage: $0 -image <img> [ -test <num> ] [ -log OFF|ALL|FINE ] [ -sleep <seconds> ]"
		exit 1
fi 

LOGS=$(pwd)/logs/
mkdir -p $LOGS

docker pull smduarte/sd18-tp1-tester

#execute the client with the given command line parameters
docker run --network=sd-net -it -v $LOGS:/logs/ -v /var/run/docker.sock:/var/run/docker.sock smduarte/sd18-tp1-tester:latest $*

#pull the logs to the host
echo "Container logs:" $LOGS
