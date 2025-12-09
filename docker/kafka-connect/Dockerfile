FROM confluentinc/cp-kafka-connect:7.5.3

# Install Confluent Hub client
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest && \
    confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:latest

# Note: Scylla CDC Source Connector would be installed via:
# RUN confluent-hub install --no-prompt scylladb/scylla-cdc-source-connector:latest
# However, since it may not be published to Confluent Hub, we'll document manual installation

# Create directory for custom connectors
RUN mkdir -p /usr/share/confluent-hub-components/scylla-cdc-source

# Set appropriate permissions
RUN chmod -R a+r /usr/share/confluent-hub-components

# JMX exporter for Prometheus metrics
ENV KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true \
                    -Dcom.sun.management.jmxremote.authenticate=false \
                    -Dcom.sun.management.jmxremote.ssl=false \
                    -Djava.rmi.server.hostname=kafka-connect \
                    -Dcom.sun.management.jmxremote.rmi.port=9999"

EXPOSE 8083 9999
