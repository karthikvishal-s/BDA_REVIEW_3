# Every target runs inside the pinned Docker image, so all four laptops behave the same.
#   make <target> INPUT=full|sample|synthetic     (default: sample)
INPUT ?= sample
HOLD  ?= 0
TASK  ?= both

DC   := docker compose -f docker/docker-compose.yml
RUN  := $(DC) run --rm --service-ports -e INPUT=$(INPUT) -e HOLD=$(HOLD) spark
JAR  := scala/target/review3-flight-spark.jar
SCALA_SRC := $(wildcard scala/src/main/scala/*.scala) scala/build.sbt
SUBMIT := spark-submit
JOB    := $(SUBMIT) --class

.PHONY: help build shell jar schema-check sample synthetic ingest rdd analytics eda features \
        train evaluate predict plots all smoke test history api query clean-outputs

help:
	@echo "make build                 build the Docker image"
	@echo "make schema-check          compare the raw CSV header with the expected columns"
	@echo "make sample                write data/samples/sample_100k.csv from the real CSV"
	@echo "make all INPUT=full        ingest -> rdd -> analytics -> eda -> features -> train -> evaluate -> predict -> plots"
	@echo "make smoke                 the whole chain on a SYNTHETIC fixture (pipeline check only)"
	@echo "make rdd HOLD=300          keep the Spark UI on :4040 alive for 300 s after the RDD demo"
	@echo "make history               Spark history server on :18080"
	@echo "make query Q='top-routes --n 10 --month 7'"

build:
	$(DC) build spark

shell:
	$(RUN) bash

jar: $(JAR)
$(JAR): $(SCALA_SRC)
	$(RUN) bash -c "cd scala && sbt -batch package"

schema-check: $(JAR)
	$(RUN) $(JOB) Ingest $(JAR) --check-only

sample:
	$(RUN) $(SUBMIT) pyspark/make_sample.py

synthetic:
	$(RUN) python3 tests/make_synthetic.py

ingest: $(JAR)
	$(RUN) $(JOB) Ingest $(JAR)

rdd: $(JAR)
	$(RUN) $(JOB) RddDemo $(JAR)

analytics: $(JAR)
	$(RUN) $(JOB) RouteAnalytics $(JAR)

eda:
	$(RUN) $(SUBMIT) pyspark/eda.py

features:
	$(RUN) $(SUBMIT) pyspark/features.py

train:
	$(RUN) $(SUBMIT) pyspark/train.py --task $(TASK)

evaluate:
	$(RUN) $(SUBMIT) pyspark/evaluate.py --task $(TASK)

predict:
	$(RUN) $(SUBMIT) pyspark/predict_demo.py

plots:
	$(RUN) python3 viz/make_plots.py

all: ingest rdd analytics eda features train evaluate predict plots

smoke: synthetic
	$(MAKE) all INPUT=synthetic

test:
	$(RUN) python3 -m pytest -q tests

history:
	$(DC) up history

api:
	$(DC) up api

query:
	$(RUN) $(SUBMIT) pyspark/live_queries.py $(Q)

clean-outputs:
	find outputs -type f ! -name .gitkeep -delete
