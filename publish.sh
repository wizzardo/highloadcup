#!/usr/bin/env bash
./gradlew clean
./gradlew fatJar
sudo docker build -t app .
sudo docker tag app stor.highloadcup.ru/travels/steep_camel
sudo docker push stor.highloadcup.ru/travels/steep_camel