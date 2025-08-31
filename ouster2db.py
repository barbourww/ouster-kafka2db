import os
import sys
import time
import json
import signal
import logging
import datetime
import zoneinfo
from typing import List

import db_laddms
from kafka_confluent import KafkaConfluentConsumer

# -------------------- Logging (console + file) --------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s"))

    file_handler = logging.FileHandler("ouster2db.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s"))

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

tz = zoneinfo.ZoneInfo('US/Central')

class Kafka2DB:
    """Consume object detection messages from Kafka and write to Postgres.

    Expects the Kafka message value to be JSON (already decoded by our consumer).
    """

    def _debug_assignment_and_lag(self, label: str = ""):
        try:
            assignment = self.consumer.consumer.assignment() or []
            parts = [f"{tp.topic}[{tp.partition}]" for tp in assignment]
            logger.info("Assignment%s: %s", f" ({label})" if label else "", ", ".join(parts) if parts else "<none>")
            try:
                status = self.consumer.lag_status(window_seconds=60)
                logger.info("Lag%s: total_messages_behind=%s", f" ({label})" if label else "", status.get("total_messages_behind"))
                for p in status.get("partitions", []):
                    logger.info("  - %s[%s]: pos=%s high=%s behind=%s older_than_60s=%s",
                                p.get("topic"), p.get("partition"), p.get("position"), p.get("high"),
                                p.get("messages_behind"), p.get("older_than_window"))
            except Exception as e:
                logger.warning("Lag status error%s: %s", f" ({label})" if label else "", e)
        except Exception as e:
            logger.warning("Assignment check error%s: %s", f" ({label})" if label else "", e)

    def __init__(self):
        # DB setup
        self.sql_connection = None
        self.sql_cursor = None

        # Device identifier (used by db_laddms insert fn)
        self.device_id = os.environ.get("DEVICE_ID", "kafka_objects")

        # Kafka setup
        self.topic = os.environ.get("KAFKA_TOPIC", "test")
        logger.info(f"Assigned topic: {self.topic}")
        kafka_conf = {
            "KAFKA_BOOTSTRAP": os.environ.get("KAFKA_BOOTSTRAP"),
            "KAFKA_USER": os.environ.get("KAFKA_USER"),
            "KAFKA_PASSWORD": os.environ.get("KAFKA_PASSWORD"),
            # Sensible defaults for this pipeline
            "KAFKA_GROUP_ID": os.environ.get("KAFKA_GROUP_ID", "ouster2db_objects"),
            "KAFKA_AUTO_OFFSET_RESET": os.environ.get("KAFKA_AUTO_OFFSET_RESET", "latest"),
            "KAFKA_ENABLE_AUTO_COMMIT": False,
            # Cap initial backlog to ~60s by default (can override via env)
            "KAFKA_MAX_START_DELAY_SECONDS": int(os.environ.get("KAFKA_MAX_START_DELAY_SECONDS", 60)),
            "KAFKA_CA_LOCATION": os.environ.get("KAFKA_CA_LOCATION", "strimzi-ca.crt"),
        }
        self.consumer = KafkaConfluentConsumer(kafka_conf)

        self._running = False

    # -------------------- Lifecycle --------------------
    def start(self):
        logger.info("Connecting to database…")
        self.sql_connection = db_laddms.make_sql_connection()
        self.sql_cursor = db_laddms.make_sql_cursor(self.sql_connection)
        logger.info("DB connection ready")

        logger.info("Subscribing to Kafka topic: %s", self.topic)
        self.consumer.subscribe([self.topic], initialize_with_poll=True)
        self._debug_assignment_and_lag("before-ff")

        # Optionally fast-forward so we don't start too far behind
        self.consumer.maybe_fast_forward(max_delay_seconds=30, reinitialize_with_poll=True)
        self._debug_assignment_and_lag("after-ff")

        self._running = True
        logger.info("Kafka2DB started; consuming from %s", self.topic)

    def stop(self):
        logger.warning("Kafka2DB STOPPED!")
        if not self._running:
            return
        logger.info("Stopping Kafka2DB…")
        try:
            self.consumer.close()
        finally:
            try:
                if self.sql_cursor:
                    self.sql_cursor.close()
                if self.sql_connection:
                    self.sql_connection.close()
            finally:
                self._running = False
                logger.info("Stopped")

    def _wait_for_assignment(self, timeout_sec: int = 10):
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            self.consumer.consumer.poll(0.1)  # triggers rebalance callbacks
            if self.consumer.consumer.assignment():
                logger.info("Kafka partitions assigned")
                return
        logger.warning("Timed out waiting for partition assignment; continuing anyway")

    # -------------------- Main consume loop --------------------
    def run_forever(self, batch_size: int = 100, poll_timeout: float = 1.0, keys_filter: List[str] = None):
        logger.info("STARTING RUN FOREVER LOOP.")
        self._debug_assignment_and_lag("loop-start")
        num_messages = {}
        num_inserts = {}

        try:
            start_time = time.time()
            while self._running:
                logger.debug("Consuming batch.")
                batch = self.consumer.consume_batch(max_messages=batch_size, timeout=poll_timeout)
                if not batch:
                    logger.info("Batch empty.")
                    continue

                logger.debug("Batch received.")
                for msg in batch:
                    try:
                        if keys_filter is not None:
                            if msg.get("key") not in keys_filter:
                                continue
                        payload = msg.get("value")
                        partition = msg.get("partition")
                        num_messages[partition] = num_messages.get(partition, 0) + 1
                        ts_utc = datetime.datetime.fromtimestamp(msg['timestamp'] / 1000,
                                                                 tz=zoneinfo.ZoneInfo("UTC"))
                        ts_local = ts_utc.astimezone(tz=zoneinfo.ZoneInfo('US/Central'))
                        lag = round((datetime.datetime.now(
                            tz=zoneinfo.ZoneInfo('US/Central')) - ts_local).total_seconds(), 3)
                        if num_messages[partition] % 100 == 0:
                            logger.info(f"Partition lag for P{partition} is {lag}s. Message counts: {num_messages}.")
                            logger.info(f"Total inserts: {num_inserts}. "
                                        f"Pace = {round(sum(num_inserts.values()) / (time.time() - start_time), 0)}/s")
                        # Our consumer tries JSON first; if producer sent text JSON, ensure dict
                        if isinstance(payload, str):
                            payload = json.loads(payload)

                        try:
                            num_query_inserts = db_laddms.insert_object_detections(
                                intersection_id=msg.get('key'),
                                timestamp_tz=msg.get('msg_timestamp_dt'),
                                json_data=payload,
                                device_id=self.device_id,
                                use_db_conn=self.sql_connection,
                                # use_db_cursor=self.sql_cursor,
                            )
                            num_inserts[partition] = num_inserts.get(partition, 0) + num_query_inserts
                        except Exception as e:
                            logger.warning("Malformed insert.", exc_info=True)
                        # Commit this message after successful DB write
                        self.consumer.commit(msg)
                    except Exception as e:
                        logger.error("Failed to process message at %s[%d] offset %s: %s",
                                     msg.get("topic"), msg.get("partition"), msg.get("offset"), e)
                        # Optionally, you could decide NOT to commit here to retry on restart
                        # For now we skip commit so it can be retried later
                        continue
                try:
                    if hasattr(self.sql_connection, "commit"):
                        self.sql_connection.commit()
                        logger.debug("Commit successful.")
                except Exception as e:
                    logger.error("Failed to commit objects detections.", exc_info=True)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received")
        finally:
            self.stop()


# -------------------- Entrypoint --------------------
if __name__ == "__main__":
    worker = Kafka2DB()

    # Graceful shutdown on SIGTERM/SIGINT
    def _graceful_shutdown(signum, frame):
        logger.info("Signal %s received; shutting down…", signum)
        worker.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    worker.start()
    worker.run_forever(keys_filter=['11', '12', '13', '14', '15'])
