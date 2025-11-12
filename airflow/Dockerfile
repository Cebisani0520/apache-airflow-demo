FROM apache/airflow:2.7.1-python3.11


USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends default-jdk python3-dev build-essential \
  && pip install --no-cache-dir pyspark==3.4.1 apache-airflow-providers-apache-spark


# create airflow home folders if missing
RUN mkdir -p /opt/spark-apps
RUN chown -R airflow: /opt/spark-apps


USER airflow
