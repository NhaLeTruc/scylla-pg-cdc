FROM confluentinc/cp-kafka-connect:7.5.3

# Install Confluent Hub client
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest && \
    confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:latest

# Install Scylla CDC Source Connector
# Download the latest stable release from GitHub (v1.2.2 has published artifacts)
RUN mkdir -p /tmp/scylla-cdc && \
    cd /tmp/scylla-cdc && \
    curl -L -o scylla-cdc-source-connector.jar \
    https://github.com/scylladb/scylla-cdc-source-connector/releases/download/v1.2.2/scylla-cdc-source-connector-1.2.2-jar-with-dependencies.jar && \
    mkdir -p /usr/share/confluent-hub-components/scylla-cdc-source && \
    mv scylla-cdc-source-connector.jar /usr/share/confluent-hub-components/scylla-cdc-source/ && \
    cd / && \
    rm -rf /tmp/scylla-cdc

# Set appropriate permissions
RUN chmod -R a+r /usr/share/confluent-hub-components

# JMX exporter for Prometheus metrics
ENV KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true \
                    -Dcom.sun.management.jmxremote.authenticate=false \
                    -Dcom.sun.management.jmxremote.ssl=false \
                    -Djava.rmi.server.hostname=kafka-connect \
                    -Dcom.sun.management.jmxremote.rmi.port=9999"

EXPOSE 8083 9999
