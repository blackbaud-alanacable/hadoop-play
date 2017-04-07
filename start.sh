#!/bin/sh
HASH=4239cd2958c6
docker images # note the hash of the image and substitute it below
docker run --privileged=true \
--hostname=quickstart.cloudera \
--name=hadoop \
--volume="/Users/alana.cable/git/otg/hadoop-play:/home/cloudera/play" \
-t -i ${HASH} \
/usr/bin/docker-quickstart
