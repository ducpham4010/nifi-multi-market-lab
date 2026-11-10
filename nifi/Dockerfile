ARG NIFI_IMAGE=apache/nifi:2.6.0
FROM ${NIFI_IMAGE}
USER root
RUN mkdir -p /opt/nifi/drivers && \
    curl -fSL --retry 3 https://jdbc.postgresql.org/download/postgresql-42.7.7.jar \
      -o /opt/nifi/drivers/postgresql.jar && \
    chmod 644 /opt/nifi/drivers/postgresql.jar
COPY --chmod=755 start-lab.sh /opt/nifi/scripts/start-lab.sh
USER nifi
ENTRYPOINT ["/opt/nifi/scripts/start-lab.sh"]
