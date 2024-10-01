# Usa una imagen base que contenga Python
FROM python:3.9

# Instala Java
RUN apt-get update && apt-get install -y openjdk-8-jdk && apt-get clean

# Configura JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Configura el directorio de trabajo
WORKDIR /app

# Copia el archivo de requerimientos e instala las dependencias
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto de la aplicación al contenedor
COPY . .

# Comando para ejecutar tu aplicación
CMD ["python", "app.py"]
