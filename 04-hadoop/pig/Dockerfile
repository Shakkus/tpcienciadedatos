FROM apache/hadoop:3.4.1

ARG PIG_VERSION=0.18.0
ENV PIG_HOME=/opt/pig
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=/opt/pig/bin:${PATH}

USER root
RUN mkdir -p "${PIG_HOME}" \
    && curl -fL --retry 3 \
        "https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz" \
        -o /tmp/pig.tar.gz \
    && tar -xzf /tmp/pig.tar.gz --strip-components=1 -C "${PIG_HOME}" \
    && rm /tmp/pig.tar.gz \
    && chmod +x "${PIG_HOME}/bin/pig"

WORKDIR /opt/nexo
ENTRYPOINT ["/opt/pig/bin/pig"]
