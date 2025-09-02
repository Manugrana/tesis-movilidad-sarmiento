# Guía de reproducibilidad

1) Crear entorno:
```
mamba env create -f environment.yml
mamba activate tesis-sarmiento
pre-commit install
```

2) Obtener datos (ver README y scripts/00_download_data.sh). No versionar `data/raw/`.

3) Ejecutar pipeline:
```
make all
```

4) Resultados y figuras en `docs/figuras/`.
