# --- Etapa de Build ---
FROM python:3.10-alpine as build

WORKDIR /app
    
# Actualizamos los índices e instalamos git
RUN apk update && apk add --no-cache git

# Creamos un entorno virtual para aislar las dependencias de forma segura
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt /app/

# Actualizamos pip y wheel para mitigar los CVEs relacionados a wheel,
# y luego instalamos los requerimientos sin cache para mantener la imagen limpia.
RUN pip install --no-cache-dir --upgrade pip wheel && \
pip install --no-cache-dir -r ./requirements.txt

# --- Etapa de Run ---
FROM python:3.10-alpine as run

# 1. Parchar vulnerabilidades del OS (como libuuid) actualizando los paquetes de Alpine
RUN apk upgrade --no-cache

# 2. Hardening: Crear un usuario y grupo sin privilegios root
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# 3. Copiamos ÚNICAMENTE el entorno virtual compilado, en lugar de todo /usr/local
COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copiamos el script de la aplicación
COPY fastagi.py /app/

# Ajustamos permisos y cambiamos el propietario al usuario no-root
RUN chmod +x /app/fastagi.py && \
chown -R appuser:appgroup /app

# 4. Cambiamos al usuario sin privilegios
USER appuser

# Definimos el entrypoint/cmd por defecto
CMD ["python", "fastagi.py"]