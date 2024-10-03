#!/bin/bash

# Crear una red para todos los servicios
docker network create recommender-network

# Levantar Redis
docker run -d --network=recommender-network --name redis redis:alpine

# Levantar Spark Master
docker run -d --network=recommender-network --name spark-master -p 7077:7077 -p 8080:8080 spark-master

# Levantar Spark Workers
docker run -d --network=recommender-network --name spark-worker-1 spark-worker
docker run -d --network=recommender-network --name spark-worker-2 spark-worker

# Levantar la aplicación Flask
docker build -t flask-app ./flask-app
docker run -d --network=recommender-network --name flask-app -p 5000:5000 flask-app
