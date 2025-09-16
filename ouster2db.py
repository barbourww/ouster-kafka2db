import os
import sys
import time
import json
import signal
import logging
from logging.handlers import TimedRotatingFileHandler
import datetime
import zoneinfo
from typing import List, Dict
import multiprocessing

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

    def __init__(self):
        # DB setup
        self.sql_connection = None
        self.sql_cursor = None

        # Device identifier (used by db_laddms insert fn)
        self.device_id = os.environ.get("DEVICE_ID", "kafka_objects")

        # Kafka setup
        self.topic = os.environ.get("KAFKA_TOPIC", "test")
        self.partitions = None
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
        base_path = os.environ.get("DATA_LOG_FILE_DIR")
        base_file = os.environ.get("DATA_LOG_FILE")
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
    def start(self, partitions: List[int], force_latest: bool):
        logger.info("Connecting to database…")
        self.sql_connection = db_laddms.make_sql_connection()
        self.sql_cursor = db_laddms.make_sql_cursor(self.sql_connection)
        logger.info("DB connection ready")

        logger.info("Subscribing to Kafka topic: %s", self.topic)
        self.consumer.subscribe([self.topic],
                                initialize_with_poll=True, init_retries=5,
                                manual_assignment=True,
                                force_latest=force_latest,
                                partitions=partitions)

        self._running = True
        self.partitions = partitions
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

    # -------------------- Main consume loop --------------------
    def run_forever(self, batch_size: int = 100, poll_timeout: float = 1.0, keys_filter: List[str] = None,
                    bypass_db_to_file: bool = False):
        logger.info("STARTING RUN FOREVER LOOP.")
        num_messages = {}
        num_inserts = {}

        if bypass_db_to_file is True:
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
                t_insert_total = 0
                for msg in batch:
                    try:
                        if keys_filter is not None:
                            if msg.get("key") not in keys_filter:
                                continue
                        payload = msg.get("value")
                        partition = msg.get("partition")
                        num_messages[partition] = num_messages.get(partition, 0) + 1
                        ts_local = msg['msg_timestamp_dt']
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
                            if bypass_db_to_file is True:
                                num_query_inserts, t_insert = self.save_data_to_file(
                                    intersection_id=msg.get('key'),
                                    timestamp_tz=msg.get('msg_timestamp_dt'),
                                    json_data=payload,
                                    device_id=self.device_id,
                                )
                            else:
                                num_query_inserts, t_insert = db_laddms.insert_object_detections(
                                    intersection_id=msg.get('key'),
                                    timestamp_tz=msg.get('msg_timestamp_dt'),
                                    json_data=payload,
                                    device_id=self.device_id,
                                    # use_db_conn=self.sql_connection,
                                    use_db_cursor=self.sql_cursor,
                                )
                            num_inserts[partition] = num_inserts.get(partition, 0) + num_query_inserts
                            t_insert_total += t_insert
                        except Exception as e:
                            logger.warning("Malformed insert.", exc_info=True)
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
                tl = time.time() - t0
                logger.info(f"Loop took {tl:.1f}s; consume took {t_consume_total:.1f}s; insert took {t_insert_total:.1f}s")
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received")
        finally:
            self.stop()


    def save_data_to_file(self, intersection_id, timestamp_tz, json_data, device_id):
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
                "device_id": device_id,
                "data": json_data,
            }
            # Write only the message (compact JSON) — no extra formatting
            self.data_logger.info(json.dumps(record, separators=(",", ":"), ensure_ascii=False))
            return object_count, time.time() - t0
        except Exception:
            # Use module-level logger for operational errors
            logger.warning("Failed to write data log entry", exc_info=True)


def process_wrapper_create_and_start(partitions: List[int], force_latest: bool):
    kdb = Kafka2DB()
    kdb.start(partitions, force_latest)
    kdb.run_forever(batch_size=300, bypass_db_to_file=False, keys_filter=['11', '12', '13', '14', '15'])


# --- helper to spawn a single worker ---
def _start_partition_process(partition: int, force_latest: bool) -> multiprocessing.Process:
    """
    Create and start a process running process_wrapper_.
    """
    p = multiprocessing.Process(
        target=process_wrapper_create_and_start,       # your existing function
        args=([partition], force_latest),
        name=f"ouster2db-p{partition}",
        daemon=False,                  # non-daemon so we can terminate/join cleanly
    )
    p.start()
    return p

# --- supervisor that launches, monitors, restarts, and handles Ctrl+C ---
def run_partitions_multiprocess(force_latest: bool) -> None:
    """
    Spawn three processes for partitions 0, 1, 2 with force_latest=True.
    Supervises children: restarts a process if it dies unexpectedly, with backoff.
    Gracefully handles KeyboardInterrupt by terminating children.
    """
    # Connect to the topic initially so that we can get
    temp_kdb = Kafka2DB()
    partitions = set(temp_kdb.consumer.get_partitions(temp_kdb.topic))
    temp_kdb.stop()
    del temp_kdb

    # Supervisor policy
    MAX_RESTARTS = None     # per-partition cap (None to disable, or int)
    BASE_BACKOFF = 1.0      # seconds
    MAX_BACKOFF = 10.0      # seconds

    # State
    procs: Dict[int, multiprocessing.Process] = {}
    restarts: Dict[int, int] = {p: 0 for p in partitions}
    backoff: Dict[int, float] = {p: BASE_BACKOFF for p in partitions}
    stopping = False

    # Start all workers
    for part in partitions:
        procs[part] = _start_partition_process(part, force_latest)
        print(f"[supervisor] started partition {part} (pid={procs[part].pid})")

    try:
        while True:
            all_done = True  # assume done unless we see a live or restarted proc

            for part in partitions:
                proc = procs.get(part)

                # Shouldn't happen, but self-heal if missing
                if proc is None:
                    procs[part] = _start_partition_process(part, force_latest)
                    print(f"[supervisor] started missing partition {part} (pid={procs[part].pid})")
                    all_done = False
                    continue

                if proc.is_alive():
                    all_done = False

                else:
                    exitcode = proc.exitcode

                    # We’re shutting down; do not restart
                    if stopping:
                        print(f"[supervisor] partition {part} exited with code {exitcode} during shutdown")
                        continue

                    # Unexpected exit: consider restart
                    if MAX_RESTARTS is not None:
                        if restarts[part] >= MAX_RESTARTS:
                            print(f"[supervisor] partition {part} exceeded max restarts ({MAX_RESTARTS}); not restarting.")
                            continue

                    delay = backoff[part]
                    print(f"[supervisor] partition {part} exited with code {exitcode}; restart after {delay:.1f}s...")
                    time.sleep(delay)

                    # Exponential backoff (clamped)
                    backoff[part] = min(backoff[part] * 2.0, MAX_BACKOFF)
                    restarts[part] += 1

                    # Start replacement
                    procs[part] = _start_partition_process(part, force_latest)
                    print(f"[supervisor] restarted partition {part} (pid={procs[part].pid}); restart #{restarts[part]}")
                    all_done = False

            if stopping:
                # STOPPING SIGNALING NOT IMPLEMENTED
                # If stopping, wait until all children are gone
                if not any(p.is_alive() for p in procs.values()):
                    break
                time.sleep(0.2)
                continue

            if all_done:
                # All workers ended naturally (no restarts pending) — exit supervisor
                break

            time.sleep(1.0)  # avoid busy loop

    except KeyboardInterrupt:
        stopping = True
        print("\n[supervisor] KeyboardInterrupt: terminating children...")
        for part, proc in procs.items():
            if proc.is_alive():
                proc.terminate()
        for part, proc in procs.items():
            proc.join()
        print("[supervisor] all child processes terminated.")

    finally:
        # Final cleanup (safety)
        for part, proc in procs.items():
            if proc.is_alive():
                try:
                    proc.terminate()
                except Exception:
                    pass
            try:
                proc.join(timeout=1.0)
            except Exception:
                pass

# --- optional: run directly as a script ---
if __name__ == "__main__":

    # Graceful shutdown on SIGTERM/SIGINT
    def _graceful_shutdown(signum, frame):
        logger.info("Signal %s received; shutting down…", signum)
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    run_partitions_multiprocess(force_latest=True)
