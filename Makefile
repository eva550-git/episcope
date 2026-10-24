.PHONY: all clean generate weather facilities counts ingest silver gold serve test

PYTHON ?= python
SF ?= 0.10
SEED ?= 42

all:
	$(PYTHON) -m pipeline.run_pipeline --sf $(SF) --seed $(SEED)

generate: weather facilities counts
	$(PYTHON) -m generator.generate_encounters --sf $(SF) --seed $(SEED)

weather:
	$(PYTHON) -m generator.fetch_weather

facilities:
	$(PYTHON) -m generator.generate_facilities --seed $(SEED)

counts: weather
	$(PYTHON) -m generator.generate_epi_counts --sf $(SF) --seed $(SEED)

ingest:
	$(PYTHON) -m pipeline.bronze.ingest_bronze

silver:
	$(PYTHON) -m pipeline.silver.clean_silver

gold:
	$(PYTHON) -m pipeline.gold.create_gold_marts

serve:
	$(PYTHON) -m pipeline.run_pipeline --skip-generation --serve

test:
	$(PYTHON) -m pytest -q tests

clean:
	rmdir /s /q lake 2>nul || true
	mkdir lake\_meta 2>nul || true
