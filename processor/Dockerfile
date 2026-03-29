FROM python:3.8-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y default-jdk gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

COPY requirements.txt .
RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    clickhouse-driver==0.2.6 \
    pandas==1.5.3 \
    pyarrow==11.0.0 \
    kafka-python==2.0.2

COPY processor/stream_processor.py .

CMD ["python", "stream_processor.py"]