#!/bin/sh
curl -T "build/libs/sql4j.war" "http://admin:admin@localhost:8080/manager/text/deploy?path=/sql4j&update=true"