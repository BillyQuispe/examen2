# Usa una imagen base que tenga Java
FROM openjdk:8-jdk-alpine

# Instala Python y las dependencias necesarias
RUN apk add --no-cache python3 py3-pip
RUN pip3 install Flask redis pyspark

# Configura JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copia tu aplicación al contenedor
COPY ./app 
WORKDIR /app

# Comando para ejecutar tu aplicación
CMD ["python3", "app.py"]
