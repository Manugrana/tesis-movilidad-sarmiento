.PHONY: env data od route bs figs all lint test

env:
	mamba env create -f environment.yml || conda env create -f environment.yml
	@echo "Activá con: mamba activate tesis-sarmiento"

data:
	scripts/00_download_data.sh

od:
	python scripts/20_build_od.py

route:
	python scripts/30_route_paths.py

bs:
	python scripts/40_compute_bs.py

figs:
	python scripts/90_make_figures.py

lint:
	pre-commit run --all-files || true

test:
	pytest -q

all: data od route bs figs
	@echo 'Pipeline completado.'
