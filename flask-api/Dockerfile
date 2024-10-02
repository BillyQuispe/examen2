# Usa una imagen base que contenga Python
FROM python:3.9-alpine

# Configura el directorio de trabajo
WORKDIR /app

# Copia el archivo de requerimientos e instala las dependencias
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto de la aplicación al contenedor
COPY . .

# Comando para ejecutar tu aplicación
CMD ["python3", "app.py"]
