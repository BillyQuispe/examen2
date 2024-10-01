# Usa una imagen base de OpenJDK
FROM openjdk:8-jdk-alpine

# Instala Python y pip
RUN apk add --no-cache python3 py3-pip

# Configura JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Establece el directorio de trabajo
WORKDIR /app

# Copia el archivo de requerimientos e instala las dependencias
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto de la aplicación al contenedor
COPY . .

# Comando para ejecutar tu aplicación
CMD ["python3", "app.py"]
