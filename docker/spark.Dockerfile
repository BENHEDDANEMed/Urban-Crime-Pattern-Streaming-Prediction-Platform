# Image Spark (S2) : Spark 3.5 officiel + Python + scikit-learn (pour DBSCAN).
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

COPY docker/spark-requirements.txt /tmp/spark-requirements.txt
RUN pip install --no-cache-dir -r /tmp/spark-requirements.txt

ENV PATH="/opt/spark/bin:${PATH}"
WORKDIR /app