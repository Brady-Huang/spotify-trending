#!/bin/bash
apt-get update
apt-get install -y docker.io docker-compose git

systemctl start docker
systemctl enable docker

git clone ${repo_url} /app
cd /app

docker-compose up -d