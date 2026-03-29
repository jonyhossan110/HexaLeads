SHELL := powershell

.PHONY: help setup-node setup-python run-scraper run-analyzer run-osint run-score run-pipeline

help:
	@echo "GeoLeadsX Makefile commands:"
	@echo "  setup-node     Install Node.js dependencies and Playwright browsers"
	@echo "  setup-python   Install Python dependencies"
	@echo "  run-scraper    Run the Playwright maps scraper"
	@echo "  run-analyzer   Run the Go website analyzer"
	@echo "  run-osint      Run the Python OSINT extractor"
	@echo "  run-score      Run the lead scoring engine"
	@echo "  run-pipeline   Run the JSON pipeline aggregator"

setup-node:
	npm install
	npx playwright install

setup-python:
	python -m pip install -r requirements.txt

run-scraper:
	node src/maps_scraper/maps_scraper.js

run-analyzer:
	go run src/go_analyzer/main.go

run-osint:
	python src/python_osint/osint.py

run-score:
	python src/scoring/score.py --input output/your_input.json

run-pipeline:
	python src/pipeline/pipeline.py
