import pandas as pd
from flask import Flask, jsonify, request
from flask_cors import CORS

# Cargar datos
movies = pd.read_csv('movies.dat', sep='::', names=['MovieID', 'Title', 'Genres'], engine='python', encoding='ISO-8859-1')
ratings = pd.read_csv('ratings.dat', sep='::', names=['UserID', 'MovieID', 'Rating', 'Timestamp'], engine='python', encoding='ISO-8859-1')
users = pd.read_csv('users.dat', sep='::', names=['UserID', 'Gender', 'Age', 'Occupation', 'Zip-code'], engine='python', encoding='ISO-8859-1')

app = Flask(__name__)
CORS(app)

# Función para calcular la distancia Manhattan
def manhattan_distance(user1_ratings, user2_ratings):
    common_movies = set(user1_ratings['MovieID']).intersection(set(user2_ratings['MovieID']))

    if not common_movies:
        return float('inf')  # No hay películas comunes

    distance = sum(
        abs(user1_ratings[user1_ratings['MovieID'] == movie]['Rating'].values[0] -
            user2_ratings[user2_ratings['MovieID'] == movie]['Rating'].values[0])
        for movie in common_movies
    )

    return distance


# Función para encontrar usuarios similares
def find_similar_users(user_id, ratings, top_n=5):
    target_user_ratings = ratings[ratings['UserID'] == user_id]

    user_distances = []

    for other_user_id in ratings['UserID'].unique():
        if other_user_id == user_id:
            continue  # No comparar con uno mismo

        other_user_ratings = ratings[ratings['UserID'] == other_user_id]
        distance = manhattan_distance(target_user_ratings, other_user_ratings)
        user_distances.append((other_user_id, distance))

    # Ordenar por distancia
    user_distances.sort(key=lambda x: x[1])

    return user_distances[:top_n]  # Retorna los n usuarios más similares


# Función para generar recomendaciones
def recommend_movies(user_id, ratings, top_n=10):
    similar_users = find_similar_users(user_id, ratings)

    # Obtener las películas que los usuarios similares han calificado
    recommended_movies = pd.DataFrame()

    for similar_user_id, _ in similar_users:
        similar_user_ratings = ratings[ratings['UserID'] == similar_user_id]
        recommended_movies = pd.concat([recommended_movies, similar_user_ratings])

    # Filtrar películas que el usuario objetivo no ha visto
    user_rated_movies = ratings[ratings['UserID'] == user_id]['MovieID']
    recommended_movies = recommended_movies[~recommended_movies['MovieID'].isin(user_rated_movies)]

    # Agrupar por MovieID y obtener el ranking promedio
    recommendations = recommended_movies.groupby('MovieID').agg({'Rating': 'mean'}).reset_index()
    recommendations = recommendations.sort_values(by='Rating', ascending=False)

    # Obtener los n mejores
    return recommendations.head(top_n)

@app.route("/recommendations/<int:user_id>", methods=["GET"])
def get_recommendations(user_id):
    recommended_movies = recommend_movies(user_id, ratings)
    recommended_movies_with_titles = recommended_movies.merge(movies, on='MovieID')

    # Convertir los datos en JSON
    recommendations_json = recommended_movies_with_titles[['Title', 'Rating', 'MovieID']].to_dict(orient='records')
    return jsonify(recommendations_json)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)