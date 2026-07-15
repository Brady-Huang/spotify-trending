FROM apache/spark:3.5.3-python3

USER root

RUN pip3 install \
    pyarrow==11.0.0 \
    pandas==1.5.3 \
    clickhouse-driver==0.2.6

USER spark