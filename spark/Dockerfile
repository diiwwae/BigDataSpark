FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu

USER root

ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/postgresql-42.7.4.jar
ADD https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3-all.jar /opt/spark/jars/clickhouse-jdbc-0.6.3.jar

ENV PYSPARK_PYTHON=python3

WORKDIR /workspace/project

CMD ["sleep", "infinity"]
