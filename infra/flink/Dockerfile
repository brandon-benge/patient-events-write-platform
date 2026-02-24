FROM flink:1.20.0

USER root

# 1. Install Python and the FULL OpenJDK
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev openjdk-11-jdk zip unzip && \
    rm -rf /var/lib/apt/lists/*

# 2. Dynamically set JAVA_HOME using a wildcard
# This works for both /usr/lib/jvm/java-11-openjdk-amd64 and ...-arm64
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-pyflink
RUN set -eux; \
  REAL_JAVA_HOME="$(ls -d /usr/lib/jvm/java-11-openjdk-* | head -n 1)"; \
  ln -s "$REAL_JAVA_HOME" "$JAVA_HOME"

# 3. Create symbolic link for python
RUN ln -s /usr/bin/python3 /usr/bin/python

# 4. Upgrade pip and install apache-flink
# We set JAVA_HOME explicitly in the same layer to ensure the installer sees it
COPY flink/requirements.txt /tmp/requirements.txt
RUN export JAVA_HOME=$JAVA_HOME && \
    pip3 install --upgrade pip && \
    pip3 install -r /tmp/requirements.txt

# Versions
ENV FLINK_VERSION=1.20.0
ENV KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ENV JDBC_CONNECTOR_VERSION=3.3.0-1.20
ENV POSTGRES_VERSION=42.7.5

RUN set -eux; \
  base="https://repo1.maven.org/maven2"; \
  wget -q -P /opt/flink/lib/ \
    "$base/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"; \
  wget -q -P /opt/flink/lib/ \
    "$base/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar"; \
  wget -q -P /opt/flink/lib/ \
    "$base/org/postgresql/postgresql/${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.jar"; \
  wget -q -P /opt/flink/lib/ \
    "$base/org/apache/flink/flink-json/${FLINK_VERSION}/flink-json-${FLINK_VERSION}.jar"

# Patch PyFlink's JDBC sink wrapper for connector-jdbc 3.x (RowJdbcOutputFormat)
# PyFlink 1.20's wrapper reflects JdbcOutputFormat which was renamed in newer JDBC connector versions.
RUN set -eux; \
  PYFLINK_ZIP="/opt/flink/opt/python/pyflink.zip"; \
  WORKDIR="/tmp/pyflink_patch"; \
  rm -rf "$WORKDIR"; \
  mkdir -p "$WORKDIR"; \
  unzip -q "$PYFLINK_ZIP" -d "$WORKDIR"; \
  JDBC_PY="$WORKDIR/pyflink/datastream/connectors/jdbc.py"; \
  test -f "$JDBC_PY"; \
  python3 -c "from pathlib import Path; import sys; p=Path(\"$JDBC_PY\"); s=p.read_text(encoding='utf-8'); s2=s.replace('JdbcOutputFormat','RowJdbcOutputFormat'); s2==s and sys.exit('Expected to find JdbcOutputFormat in jdbc.py but did not.'); p.write_text(s2, encoding='utf-8'); print('Patched', p)"; \
  cd "$WORKDIR"; \
  rm -f "$PYFLINK_ZIP"; \
  zip -qr "$PYFLINK_ZIP" pyflink; \
  rm -rf "$WORKDIR"; \
  echo "Patched $PYFLINK_ZIP"