import os
import sys
import time
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import datetime
import zoneinfo
from dotenv import load_dotenv
load_dotenv()

from kafka_confluent import KafkaConfluentConsumer

# -------------------- Logging (console + file) --------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s"))

    file_handler = logging.FileHandler("ousterkafkafeed.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s"))

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

tz = zoneinfo.ZoneInfo('US/Central')

class OusterKafkaFeed:
    """Consume object detection messages from Kafka and write to Postgres.

    Expects the Kafka message value to be JSON (already decoded by our consumer).
    """

    def __init__(self, intersection_ids, topic,
                 auto_offset_reset='latest', max_start_delay_seconds=60,
                 data_log_directory="", data_log_filename=""):
        self.intersection_id_filter = intersection_ids

        # Kafka setup
        self.topic = topic
        self.partitions = None
        logger.info(f"Assigned topic: {self.topic}")
        kafka_conf = {
            "KAFKA_BOOTSTRAP": os.environ.get("KAFKA_BOOTSTRAP"),
            "KAFKA_USER": os.environ.get("KAFKA_USER"),
            "KAFKA_PASSWORD": os.environ.get("KAFKA_PASSWORD"),
            # Sensible defaults for this pipeline
            "KAFKA_GROUP_ID": os.environ.get("KAFKA_GROUP_ID", "OusterStreamConsumer"),
            "KAFKA_AUTO_OFFSET_RESET": auto_offset_reset,
            "KAFKA_ENABLE_AUTO_COMMIT": False,
            # Cap initial backlog to ~60s by default (can override via env)
            "KAFKA_MAX_START_DELAY_SECONDS": max_start_delay_seconds,
            "KAFKA_CA_LOCATION": os.environ.get("KAFKA_CA_LOCATION", "/etc/viewlive/certs/strimzi-ca.crt"),
        }
        self.consumer = KafkaConfluentConsumer(kafka_conf)

        # Data logger parameters -- only used if enabled in run_forever()
        self.data_log_directory = data_log_directory
        self.data_log_filename = data_log_filename

        # Instance-specific data logger (15-minute rotating file)
        self.data_logger = None
        self._running = False


    def _setup_data_logger(self):
        """Create/refresh an instance-specific rotating file logger using self.partitions.
        - Rotates every 15 minutes.
        - Writes only raw message text (no extra formatting).
        - Logger/file name are derived from the partition list so multiple processes don't collide.
        """
        # Stable partition label: p0, p1-2, p0-1-2, or 'all'
        parts = [] if not self.partitions else sorted(self.partitions)
        part_label = "all" if not parts else "-".join(str(p) for p in parts)

        # Allow env overrides; fall back to previous default stem
        base_path = self.data_log_directory
        base_file = self.data_log_filename
        # Ensure a .log suffix while preserving any directory components
        base_root = base_file[:-4] if base_file.endswith(".log") else base_file
        log_path = os.path.join(base_path, f"{base_root}_p{part_label}.log")

        # Unique per partition set (and instance) to avoid cross-talk
        logger_name = f"Kafka2DBData.p{part_label}.{id(self)}"

        dl = logging.getLogger(logger_name)
        dl.setLevel(logging.INFO)
        dl.propagate = False

        # Clear existing handlers if reconfigured (e.g., restart)
        for h in list(dl.handlers):
            try:
                h.close()
            except Exception:
                pass
            dl.removeHandler(h)

        handler = TimedRotatingFileHandler(
            log_path,
            when="M",
            interval=15,
            backupCount=int(os.environ.get("DATA_LOG_BACKUPS", "960")),
            encoding="utf-8",
        )
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))

        dl.addHandler(handler)
        self.data_logger = dl


    # -------------------- Lifecycle --------------------
    def start(self, force_latest: bool, target_partition: int = None):
        logger.info("Subscribing to Kafka topic: %s", self.topic)
        if target_partition is not None:
            self.partitions = [target_partition]
        else:
            self.partitions = list(set(self.consumer.get_all_partitions(self.topic)))
            logger.info("Found partition list: %s", self.partitions)

        self.consumer.subscribe([self.topic],
                                initialize_with_poll=True, init_retries=5,
                                manual_assignment=True,
                                force_latest=force_latest,
                                partitions=self.partitions)

        self._running = True
        logger.info("OusterKafkaFeed started; consuming from %s", self.topic)


    def stop(self):
        logger.warning("OusterKafkaFeed STOPPED!")
        if not self._running:
            return
        logger.info("Stopping OusterKafkaFeed...")
        try:
            self.consumer.close()
        finally:
            self._running = False
            logger.info("Stopped")

    # -------------------- Main consume loop --------------------
    def run_forever(self, batch_size: int = 100, poll_timeout: float = 1.0,
                    save_to_data_log_file: bool = False):
        logger.info("STARTING RUN FOREVER LOOP.")
        num_messages = {}

        if save_to_data_log_file is True:
            if self.data_logger is None:
                self._setup_data_logger()

        try:
            start_time = time.time()
            while self._running:
                logger.debug("Consuming batch.")
                batch, t_consume_receive, t_consume_total = self.consumer.consume_batch(
                    max_messages=batch_size, timeout=poll_timeout)
                if not batch:
                    logger.info("Batch empty.")
                    continue

                logger.debug("Batch received.")
                t0 = time.time()
                processing_time = 0
                for msg in batch:
                    try:
                        if self.intersection_id_filter is not None:
                            if msg.get("key") not in self.intersection_id_filter:
                                continue
                        payload = msg.get("value")
                        partition = msg.get("partition")
                        num_messages[partition] = num_messages.get(partition, 0) + 1
                        ts_local = msg['msg_timestamp_dt']
                        lag = round((datetime.datetime.now(
                            tz=zoneinfo.ZoneInfo('US/Central')) - ts_local).total_seconds(), 3)
                        if num_messages[partition] % 100 == 0:
                            logger.info(f"Partition lag for P{partition} is {lag}s. Message counts: {num_messages}.")
                        # Our consumer tries JSON first; if producer sent text JSON, ensure dict
                        if isinstance(payload, str):
                            payload = json.loads(payload)

                        try:
                            if save_to_data_log_file is True:
                                num_query_inserts, t_insert = self.save_data_to_file(
                                    intersection_id=msg.get('key'),
                                    timestamp_tz=msg.get('msg_timestamp_dt'),
                                    json_data=payload,
                                )
                            t1 = time.time()
                            self.process_new_message(
                                intersection_id=msg.get('key'),
                                timestamp_tz=msg.get('msg_timestamp_dt'),
                                json_data=payload,
                            )
                            processing_time += time.time() - t1
                        except Exception as e:
                            logger.warning("Exception in save/processing.", exc_info=True)
                    except Exception as e:
                        logger.error("Failed to process message at %s[%d] offset %s: %s",
                                     msg.get("topic"), msg.get("partition"), msg.get("offset"), e)
                        # Optionally, you could decide NOT to commit here to retry on restart
                        # For now we skip commit so it can be retried later
                        continue
                tl = time.time() - t0
                logger.info(f"Batch ({len(batch)} msg) loop took {tl:.2f}s; consume took {t_consume_total:.2f}s; processing took {processing_time:.2f}s")
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received")
        finally:
            self.stop()


    def save_data_to_file(self, intersection_id, timestamp_tz, json_data):
        """Persist a single JSON line with inputs + payload via the instance data logger.
        The file rotates every 15 minutes per TimedRotatingFileHandler.
        """
        try:
            t0 = time.time()
            if 'object_list' not in json_data:
                return 0, 0
            else:
                object_count = len(json_data["object_list"][0]["objects"])
            # Normalize timestamp for JSON serialization
            if hasattr(timestamp_tz, "isoformat"):
                ts = timestamp_tz.isoformat()
            else:
                ts = str(timestamp_tz)

            record = {
                "intersection_id": intersection_id,
                "timestamp": ts,
                "data": json_data,
            }
            # Write only the message (compact JSON) — no extra formatting
            self.data_logger.info(json.dumps(record, separators=(",", ":"), ensure_ascii=False))
            return object_count, time.time() - t0
        except Exception:
            # Use module-level logger for operational errors
            logger.warning("Failed to write data log entry", exc_info=True)


    def process_new_message(self, intersection_id, timestamp_tz, json_data):
        pass



# --- optional: run directly as a script ---
if __name__ == "__main__":
    kdb = OusterKafkaFeed(intersection_ids=['17'], topic='my-topic')
    target_partition = kdb.consumer.discover_partition_for_key('my-topic', '17')
    logger.info(f"Found target partition: {target_partition}")
    kdb = OusterKafkaFeed(intersection_ids=['17'], topic='my-topic',
                          data_log_directory='./', data_log_filename='datastream.log')
    kdb.start(force_latest=True, target_partition=target_partition)
    kdb.run_forever(batch_size=100, save_to_data_log_file=True)
