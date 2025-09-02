#!/usr/bin/env bash
set -euo pipefail

# Crear carpeta si no existe
mkdir -p data/raw

# Descargar transacciones desde el repo oficial del BID
echo "Descargando transacciones SUBE..."
curl -L \
  https://raw.githubusercontent.com/EL-BID/Matriz-Origen-Destino-Transporte-Publico/main/data/transacciones.csv \
  -o data/raw/transacciones.txt

echo "Datos guardados en data/raw/transacciones.txt"

