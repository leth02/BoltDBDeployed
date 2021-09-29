#Create ubuntu as base image
FROM ubuntu:18.04

#Install packages
RUN apt-get update && apt-get install -y \
    apt-utils \
    golang \
    golang-github-boltdb-bolt-dev \