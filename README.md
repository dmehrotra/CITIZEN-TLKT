cnf ?= config.env
include $(cnf)
export $(shell sed 's/=.*//' $(cnf))


.PHONY: help

help: ## List Citizen TLKT make tasks
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

check_status: ## Check the status of the scraped data
	python3 run.py check_status --data_path "data"

scrape_map: ## Scraped whats visible on the map and update the manifest
	python3 run.py scrape_map --data_path "data"

scrape_24h_logs: ## Get Incident Updates from the last day
	python3 run.py scrape_map --data_path "data"
	python3 run.py scrape_incidents --data_path "data" --timeframe "24h" 

scrape_48h_logs: ## Get Incident Updates from the last day
	python3 run.py scrape_incidents --data_path "data" --timeframe "48h" 

scrape_logs: ## Get Incident Updates
	python3 run.py scrape_incidents --data_path "data" --timeframe "all" 

compile_logs: 
	python3 run.py compile_logs --data_path "data/logs" --out_path "data/outputs"

pull_remote_logs: ## this only works if you have given dhruv your id_rsa.pub. Saves to data/output/
	scp -r root@139.59.147.91:~/tlkt/CTZN-TLKT/data/outputs/citizen_incidents.csv ./data/outputs/citizen_incidents.csv

check_audio_progress: #Checks the Status of Media downloads by comparing the citizen incidents you've scraped with the audio files you've downloaded. It spits out a manifest that can be used later to pull new audio.
	python3 run.py check_audio_progress --data_path "data/outputs" --audio_path "/Volumes/easystore/Citizen/data/audio" 

pull_dispatch_audio:
	make check_audio_progress
	python3 run.py pull_dispatch --data_path "data/outputs" 

pull_dispatch_text:
# 		make check_audio_progress
		python3 run.py transcribe_dispatch --data_path "data/outputs"  --open_ai_key ${OPEN_AI_KEY}
