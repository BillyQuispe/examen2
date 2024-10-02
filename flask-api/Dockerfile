# Usa una imagen base que contenga Python
FROM python:3.9-alpine

# Instala Java (OpenJDK 11)
RUN apk add --no-cache openjdk11

# Configura JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Configura el directorio de trabajo
WORKDIR /app

# Copia el archivo de requerimientos e instala las dependencias
COPY requirements.txt ./  # Copia el archivo de requerimientos
RUN pip install --no-cache-dir -r requirements.txt  # Instala las dependencias

# Copia el resto de la aplicación al contenedor
COPY . .  # Copia todos los archivos de tu aplicación

# Comando para ejecutar tu aplicación
CMD ["python3", "app.py"]
