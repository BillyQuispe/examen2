FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copiar la aplicación Flask
COPY . .

# Comando para ejecutar la aplicación Flask
CMD ["python", "app.py"]
