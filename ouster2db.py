import os
import sys
import time
import json
import signal
import logging
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
    def run_forever(self, batch_size: int = 100, poll_timeout: float = 1.0, keys_filter: List[str] = None):
        logger.info("STARTING RUN FOREVER LOOP.")
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
                                # use_db_conn=self.sql_connection,
                                use_db_cursor=self.sql_cursor,
                            )
                            num_inserts[partition] = num_inserts.get(partition, 0) + num_query_inserts
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
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received")
        finally:
            self.stop()


def process_wrapper_create_and_start(partitions: List[int], force_latest: bool):
    kdb = Kafka2DB()
    kdb.start(partitions, force_latest)
    kdb.run_forever()


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
