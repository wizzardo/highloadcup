#!/usr/bin/env bash
./gradlew clean
./gradlew fatJar
sudo docker build -t app .
sudo docker run --rm -p 8080:80 -t app