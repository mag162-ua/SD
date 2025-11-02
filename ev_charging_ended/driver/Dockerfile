# Usamos Python 3.11
FROM python:3.11-slim

# Directorio de trabajo
WORKDIR /app

# Copiamos las dependencias
COPY app/requirements.txt .

# Instalamos dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos la aplicación
COPY app/ .

# Puerto en el que escuchará Central (puede parametrizarse)
EXPOSE 5000

# Comando por defecto para ejecutar Central
CMD ["python", "ev_driver.py"]