Esta carpeta organiza todos los datos utilizados en el proyecto de tesis.  
La idea es mantener un flujo claro: **raw → interim → processed**, además de datos externos de referencia.

⚠️ **Importante**: por motivos de peso y privacidad, los archivos grandes (como las transacciones SUBE) no se versionan en GitHub.  
En su lugar, se proveen scripts para descargarlos y procesarlos.

---

## Estructura

- **`raw/`**  
  Contiene los insumos originales, sin modificar.  
  - `transacciones.txt`: dataset de viajes SUBE, descargado automáticamente desde el repositorio del BID:  
    [EL-BID/Matriz-Origen-Destino-Transporte-Publico](https://github.com/EL-BID/Matriz-Origen-Destino-Transporte-Publico).  
  - Otros datos brutos (ej. shapefiles de GCBA, capas OSM).

- **`external/`**  
  Datos complementarios de fuentes externas (ej. capas de comunas, escuelas, traza del ferrocarril Sarmiento).  
  No se alteran, solo se usan como referencia.

- **`interim/`**  
  Datos intermedios luego de procesos de limpieza o filtrado.  
  Ejemplo:  
  - `cleaned.parquet`: transacciones SUBE filtradas (solo estudiantes primarios).

- **`processed/`**  
  Resultados listos para el análisis y visualización.  
  Ejemplo:  
  - `od_pairs.parquet`: pares hogar–escuela inferidos.  
  - `routes.geojson`: trayectorias casa–escuela generadas con OSMnx.  
  - `bs_results.parquet`: métricas Barrier Score calculadas.

---

## Reproducibilidad

Para obtener los datos en `raw/`:

```bash
make data
