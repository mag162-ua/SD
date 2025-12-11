# Usa una imagen base de Python ligera para reducir el tamaño
FROM python:3.11-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia el archivo de dependencias e instálalas
COPY app/requirements.txt .
# --no-cache-dir para ahorrar espacio de almacenamiento en la imagen
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código fuente de tu aplicación (EV_Registry.py) al contenedor
COPY /app .

# El puerto 5000 es el que se usa internamente en el código de Registry.py
EXPOSE 8080

# Comando para arrancar el servidor Flask.
# El script EV_Registry.py contiene el 'if __name__ == "__main__":' 
# que arranca la aplicación Flask y el contexto SSL (HTTPS).
CMD ["python", "EV_Registry.py"]