from flask import Flask, jsonify
from flask_cors import CORS
from pyspark.sql import SparkSession
import redis
import json
import os

app = Flask(__name__)
CORS(app)

# Crear sesión de Spark distribuido
spark = SparkSession.builder \
    .appName("MovieRecommendation") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

# Conectar a Redis
redis_host = os.getenv("REDIS_HOST", "redis")
redis_port = 6379
cache = redis.Redis(host=redis_host, port=redis_port)

# Cargar datos
ratings = spark.read.csv('/data/ratings.dat', sep='::', header=True, inferSchema=True)
movies = spark.read.csv('/data/movies.dat', sep='::', header=True, inferSchema=True)
users = spark.read.csv('/data/users.dat', sep='::', header=True, inferSchema=True)

# Función para calcular la distancia Manhattan
def manhattan_distance(user1_ratings, user2_ratings):
    common_movies = user1_ratings.join(user2_ratings, 'MovieID')
    if common_movies.rdd.isEmpty():
        return float('inf')
    distance = common_movies.rdd.map(lambda row: abs(row['Rating_x'] - row['Rating_y'])).sum()
    return distance

def find_similar_users(user_id, ratings, top_n=5):
    target_user_ratings = ratings.filter(ratings['UserID'] == user_id)
    other_user_ids = ratings.select('UserID').distinct().collect()

    user_distances = []
    for other_user in other_user_ids:
        other_user_id = other_user['UserID']
        if other_user_id == user_id:
            continue

        other_user_ratings = ratings.filter(ratings['UserID'] == other_user_id)
        distance = manhattan_distance(target_user_ratings, other_user_ratings)
        user_distances.append((other_user_id, distance))

    user_distances.sort(key=lambda x: x[1])
    return user_distances[:top_n]

@app.route("/recommendations/<int:user_id>", methods=["GET"])
def get_recommendations(user_id):
    cached_recommendations = cache.get(f'recommendations:{user_id}')
    if cached_recommendations:
        return jsonify(json.loads(cached_recommendations))

    similar_users = find_similar_users(user_id, ratings)
    recommended_movies = []
    for similar_user_id, _ in similar_users:
        similar_user_ratings = ratings.filter(ratings['UserID'] == similar_user_id)
        recommended_movies.extend(similar_user_ratings.collect())

    recommendations_df = spark.createDataFrame(recommended_movies)
    recommendations = recommendations_df.groupBy("MovieID").agg({"Rating": "mean"}).orderBy("mean(Rating)", ascending=False)
    recommendations_json = recommendations.toPandas().to_dict(orient='records')

    cache.set(f'recommendations:{user_id}', json.dumps(recommendations_json))
    return jsonify(recommendations_json)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
