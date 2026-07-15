import json
import logging
from pyspark.sql.streaming import StreamingQueryListener
from prometheus_client import start_http_server, Gauge

logger = logging.getLogger("SpotifyProcessor")

BATCH_DURATION = Gauge('spark_batch_duration_ms', 'Spark batch duration in ms')
INPUT_ROWS_PER_SEC = Gauge('spark_input_rows_per_second', 'Input rows per second from Kafka')
PROCESSED_ROWS_PER_SEC = Gauge('spark_processed_rows_per_second', 'Processed rows per second')
KAFKA_START_OFFSET = Gauge('spark_kafka_start_offset', 'Kafka start offset per batch')
KAFKA_END_OFFSET = Gauge('spark_kafka_end_offset', 'Kafka end offset per batch')


class PrometheusStreamingListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        logger.info(f"Streaming query started: {event.id}")

    def onQueryProgress(self, event):
        progress = event.progress
        BATCH_DURATION.set(progress.batchDuration)
        INPUT_ROWS_PER_SEC.set(progress.inputRowsPerSecond)
        PROCESSED_ROWS_PER_SEC.set(progress.processedRowsPerSecond)

        sources = progress.sources
        if sources:
            source = sources[0]
            try:
                if source.startOffset:
                    start = json.loads(source.startOffset)
                    offset_val = list(list(start.values())[0].values())[0]
                    KAFKA_START_OFFSET.set(offset_val)
                if source.endOffset:
                    end = json.loads(source.endOffset)
                    offset_val = list(list(end.values())[0].values())[0]
                    KAFKA_END_OFFSET.set(offset_val)
            except Exception as e:
                logger.warning(f"Failed to parse offsets: {e}")

    def onQueryTerminated(self, event):
        logger.info(f"Streaming query terminated: {event.id}")


def start_metrics_server(port=8888):
    start_http_server(port)
    logger.info(f"Prometheus metrics server started on port {port}")
