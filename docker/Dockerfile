# Review 3 — one image for every laptop (macOS, WSL2, Ubuntu).
# Spark 3.5.3 + Scala 2.12 + JDK 11 + Python 3, plus sbt and the pinned Python packages.
FROM spark:3.5.3-scala2.12-java11-python3-ubuntu

USER root

ARG SBT_VERSION=1.10.2

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl make python3-pip \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
    | tar -xz -C /opt \
 && ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt

COPY docker/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

COPY docker/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY docker/log4j2.properties /opt/spark/conf/log4j2.properties

ENV SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:/opt/sbt/bin:$PATH \
    PYSPARK_PYTHON=python3 \
    PROJECT_ROOT=/work \
    PYTHONHASHSEED=0 \
    MPLBACKEND=Agg

# Lets plain `python3` / pytest import pyspark without going through spark-submit.
RUN echo "/opt/spark/python" > "$(python3 -c 'import site; print(site.getsitepackages()[0])')/spark.pth" \
 && ls /opt/spark/python/lib/py4j-*-src.zip > /tmp/py4j \
 && cat /tmp/py4j >> "$(python3 -c 'import site; print(site.getsitepackages()[0])')/spark.pth"

WORKDIR /work
ENTRYPOINT []
CMD ["bash"]
