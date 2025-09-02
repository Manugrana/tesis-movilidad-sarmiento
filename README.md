# Tesis — Movilidad escolar y efecto barrera del tren Sarmiento (CABA)

Este repositorio contiene el código y los pasos para reproducir (en lo posible) los análisis de la tesis de licenciatura sobre trayectorias casa–escuela con datos SUBE y la métrica **Barrier Score**.

## Reproducir en tu máquina

```bash
mamba env create -f environment.yml
mamba activate tesis-sarmiento
pre-commit install
make all
```

## Estructura

- `data/raw/`, `data/external/`, `data/interim/`, `data/processed/`
- `src/` módulos reutilizables (IO, limpieza, OD, ruteo OSMnx, métricas BS)
- `scripts/` entrypoints del pipeline (ingesta → limpieza → OD → ruteo → BS → figuras)
- `notebooks/` análisis exploratorios/narrativa (usa funciones de `src/`)
- `docs/` guía de reproducibilidad + figuras finales

## Cómo citar

Si usás este repositorio, por favor citá la tesis (ver `CITATION.cff`) y el release DOI (cuando publiques un Release enlazado a Zenodo).
