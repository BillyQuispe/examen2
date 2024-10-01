from flask import Flask, jsonify
from flask_cors import CORS  # Importa CORS
from pyspark.sql import SparkSession
import redis
import json

app = Flask(__name__)
CORS(app)  # Habilita CORS para toda la aplicación

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("MovieRecommendation") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Conectar a Redis
redis_host = "redis"
redis_port = 6379
cache = redis.Redis(host=redis_host, port=redis_port)

# Cargar datos
ratings = spark.read.csv('/data/ratings.dat', sep='::', header=True, inferSchema=True)
movies = spark.read.csv('/data/movies.dat', sep='::', header=True, inferSchema=True)
users = spark.read.csv('/data/users.dat', sep='::', header=True, inferSchema=True)  # Cargar usuarios

def manhattan_distance(user1_ratings, user2_ratings):
    common_movies = user1_ratings.join(user2_ratings, 'MovieID')
    if common_movies.isEmpty():
        return float('inf')  # No hay películas comunes
    distance = common_movies.rdd.map(lambda row: abs(row['Rating_x'] - row['Rating_y'])).sum()
    return distance

def find_similar_users(user_id, ratings, top_n=5):
    target_user_ratings = ratings.filter(ratings['UserID'] == user_id)

    user_distances = []
    other_user_ids = ratings.select('UserID').distinct().collect()

    for other_user in other_user_ids:
        other_user_id = other_user['UserID']
        if other_user_id == user_id:
            continue  # No comparar con uno mismo

        other_user_ratings = ratings.filter(ratings['UserID'] == other_user_id)
        distance = manhattan_distance(target_user_ratings, other_user_ratings)
        user_distances.append((other_user_id, distance))

    user_distances.sort(key=lambda x: x[1])
    return user_distances[:top_n]

@app.route("/recommendations/<int:user_id>", methods=["GET"])
def get_recommendations(user_id):
    # Verificar si hay recomendaciones en caché
    cached_recommendations = cache.get(f'recommendations:{user_id}')
    if cached_recommendations:
        return jsonify(json.loads(cached_recommendations))

    # Calcular recomendaciones
    similar_users = find_similar_users(user_id, ratings)

    recommended_movies = []
    for similar_user_id, _ in similar_users:
        similar_user_ratings = ratings.filter(ratings['UserID'] == similar_user_id)
        recommended_movies.extend(similar_user_ratings.collect())

    recommendations_df = spark.createDataFrame(recommended_movies)
    recommendations = recommendations_df.groupBy("MovieID").agg({"Rating": "mean"}).orderBy("mean(Rating)", ascending=False)

    recommendations_json = recommendations.toPandas().to_dict(orient='records')

    # Almacenar recomendaciones en caché
    cache.set(f'recommendations:{user_id}', json.dumps(recommendations_json))

    return jsonify(recommendations_json)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
