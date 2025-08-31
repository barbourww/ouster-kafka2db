import json
import os
import sys
import logging
import time
import datetime
import zoneinfo
from typing import Iterable, List, Optional, Tuple, Dict, Any
import threading
from concurrent.futures import ThreadPoolExecutor, Future

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Avoid adding handlers twice if this module gets imported multiple times
if not logger.handlers:
    # Console (stdout)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s"))

    # File
    fh = logging.FileHandler("ouster2db.log")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s"))

    # Add both
    logger.addHandler(ch)
    logger.addHandler(fh)


class KafkaConfluentConsumer:
    """
    Thin wrapper around confluent_kafka.Consumer with sane defaults for
    SASL_SSL/SCRAM-SHA-512 clusters and helper methods for message handoff.

    Typical use in another module:
        consumer = KafkaConfluentConsumer({ ... })
        consumer.subscribe(["ouster.lidar"])
        while True:
            msg = consumer.poll()
            if not msg:
                continue
            # msg_dict contains decoded payload and metadata
            db_write(msg)
            consumer.commit(msg["_raw"])  # commit after successful write
    """

    def __init__(self, config: Dict[str, Any]):
        # Required config
        bootstrap = config["KAFKA_BOOTSTRAP"]
        username = config["KAFKA_USER"]
        password = config["KAFKA_PASSWORD"]

        # Optional overrides
        group_id = config.get("KAFKA_GROUP_ID", "ouster2pg_consumer")
        auto_offset_reset = config.get("KAFKA_AUTO_OFFSET_RESET", "latest")
        # enable_auto_commit = bool(config.get("KAFKA_ENABLE_AUTO_COMMIT", True))
        ca_location = config.get("KAFKA_CA_LOCATION", "strimzi-ca.crt")
        self._max_start_delay_seconds = int(config.get("KAFKA_MAX_START_DELAY_SECONDS", 60))

        self.conf = {
            "bootstrap.servers": bootstrap,
            "security.protocol": "SASL_SSL",
            "ssl.ca.location": ca_location,
            'ssl.endpoint.identification.algorithm': 'none',
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": username,
            "sasl.password": password,
            # Set a standard group ID for these consumers, so that if we create more of them they will
            #   be placed in the same group and work together.
            "group.id": group_id,
            # Use new consumer protocol, which does broker-side balancing and assignment.
            "group.protocol": "consumer",
            # Default seek to latest. Option to seek backwards in log, but default to front.
            "auto.offset.reset": auto_offset_reset,
            # No guaranteed delivery of messages right now.
            "enable.auto.commit": True,
            "enable.auto.offset.store": True,
            # If you want faster failover at the cost of more polls, tweak session/heartbeat
            # "session.timeout.ms": 45000,
            # "heartbeat.interval.ms": 15000,
            # pull more data per request (overall / per partition)
            "fetch.max.bytes": 100_000_000,  # ~50 MB total per fetch
            "max.partition.fetch.bytes": 80_000_000,  # ~20 MB per partition fetch (topic/broker must allow)
            # keep the pipe busy, reduce latency
            "fetch.wait.max.ms": 50,
            "queued.max.messages.kbytes": 512_000,  # ~512 MB local queue budget (tune downward if RAM-limited)
            # safety: accept larger messages (default is usually 100MB)
            "receive.message.max.bytes": 200_000_000,
        }

        try:
            self.consumer = Consumer(self.conf)
        except Exception as e:
            logger.exception("Failed to create Kafka consumer: %s", e)
            raise

        self._subscribed = False
        self._executor = ThreadPoolExecutor(max_workers=2)

    # ---- Subscription / lifecycle -------------------------------------------------
    def get_partitions(self, topic: str):
        md = self.consumer.list_topics(topic=topic, timeout=10.0)
        partitions = list(md.topics[topic].partitions.keys())  # [0,1,2,...]
        logger.info(f"\tAvailable partitions for topic {topic}: {partitions}")
        return [TopicPartition(topic, p) for p in partitions]

    def subscribe(self, topics: List[str], initialize_with_poll: bool = True, init_retries: int = 5,
                  manual_assignment: bool = False, partitions: List[int] = None) -> None:
        """Subscribe to one or more topics, or explicitly assign a single partition.

        Args:
            topics: List of topic names. If `manual_assignment=True` and`partition` is provided,
                        exactly one topic must be given.
            initialize_with_poll: If True, perform a short initialization poll after subscribing/assigning.
            init_retries: Number of attempts during the initialization poll.
            manual_assignment: Let Kafka do assignment or try to force manual partition assignment to consumer.
            partitions: If provided, explicitly assign to this partition of the single provided topic
                       (bypasses group management / rebalance callbacks).
        """
        if not topics:
            raise ValueError("subscribe() requires at least one topic")
        if len(topics) > 1 and partitions is not None:
            raise ValueError("Multiple topics passed with a single partition specified.")
        self.consumer.subscribe(topics, on_assign=self._on_assign, on_revoke=self._on_revoke)
        self._subscribed = True
        logger.info("Subscribed to topics: %s", topics)
        if manual_assignment is True:
            for this_topic in topics:
                if partitions is not None:
                    self.consumer.assign([TopicPartition(topic, p) for p in partitions])
                else:
                    this_topic_partitions = self.get_partitions(topic=this_topic)
                    self.consumer.assign(this_topic_partitions)
        if initialize_with_poll is True:
            self.initialize_poll(retries=init_retries)

    def close(self) -> None:
        try:
            self.consumer.close()
            self._executor.shutdown(wait=True)
        except Exception:
            logger.exception("Error closing consumer", exc_info=True)

    # ---- Polling ------------------------------------------------------------------
    def poll(self, timeout: float = 1.0, convert_msg_timestamp_dt: bool = True,
             timestamp_tz: zoneinfo.ZoneInfo = zoneinfo.ZoneInfo('US/Central')) -> Optional[Dict[str, Any]]:
        """
        Poll for a single message. Returns a dict with decoded payload & metadata,
        or None if no message is available within timeout.
        The original Message object is returned under key "_raw" for commit().
        """
        if not self._subscribed:
            raise RuntimeError("poll() called before subscribe()")

        try:
            rcv_msg = self.consumer.poll(timeout)
        except KafkaException as e:
            logger.error("Kafka poll exception: %s", e)
            return None

        if rcv_msg is None:
            return None

        if rcv_msg.error():
            # Some errors are informational (e.g., partition EOF). Surface only real errors.
            if rcv_msg.error().code() == KafkaError._PARTITION_EOF:
                logger.debug("Partition EOF: %s", rcv_msg.error())
                return None
            logger.error("Kafka message error: %s", rcv_msg.error())
            return None

        msg_dict = self._to_dict(rcv_msg)
        if convert_msg_timestamp_dt is True:
            msg_ts_utc = datetime.datetime.fromtimestamp(msg_dict['timestamp'] / 1000, tz=zoneinfo.ZoneInfo("UTC"))
            msg_ts_local = msg_ts_utc.astimezone(tz=timestamp_tz)
            msg_dict['msg_timestamp_dt'] = msg_ts_local

        return msg_dict

    def initialize_poll(self, retries: int = 5, error_on_failure: bool = True) -> None:
        for i in range(retries):
            test_msg = self.poll(timeout=5.0, convert_msg_timestamp_dt=True)
            if test_msg:
                logger.info("Received initial message for initialization:")
                print(f"\tMessage key: {test_msg['key']}")
                print(f"\tMessage timestamp: {test_msg['msg_timestamp_dt']}")
                break
            else:
                print(f"No message on poll attempt #{i + 1}.")
        else:
            print("No message received in multiple attempts")
            self.close()
            self._subscribed = False
            if error_on_failure:
                raise ConnectionError(f"Could not receive data during initialization poll on {retries} retries.")

    def consume_batch(self, max_messages: int = 100, timeout: float = 1.0, convert_msg_timestamp_dt: bool = True,
             timestamp_tz: zoneinfo.ZoneInfo = zoneinfo.ZoneInfo('US/Central')) -> List[Dict[str, Any]]:
        """Consume up to max_messages in a batch for higher throughput."""
        if not self._subscribed:
            raise RuntimeError("consume_batch() called before subscribe()")

        try:
            msgs = self.consumer.consume(num_messages=max_messages, timeout=timeout)
        except KafkaException as e:
            logger.error("Kafka consume exception: %s", e)
            return []

        out = []
        for m in msgs or []:
            if m is None or m.error():
                if m and m.error() and m.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka message error in batch: %s", m.error())
                continue
            msg_dict = self._to_dict(m)
            if convert_msg_timestamp_dt is True:
                msg_ts_utc = datetime.datetime.fromtimestamp(msg_dict['timestamp'] / 1000, tz=zoneinfo.ZoneInfo("UTC"))
                msg_ts_local = msg_ts_utc.astimezone(tz=timestamp_tz)
                msg_dict['msg_timestamp_dt'] = msg_ts_local
            out.append(msg_dict)
        return out

    def get_lag_status_from_kafka_async(self, assignment) -> Dict[str, Any]:
        report = {"partitions": [], "total_messages_behind": 0}

        for idx, tp in enumerate(assignment):
            try:
                low, high = self.consumer.get_watermark_offsets(tp, timeout=10.0)
                pos_tp = self.consumer.position([tp])
                current = pos_tp[0].offset if pos_tp and pos_tp[0] and pos_tp[0].offset is not None else low
                msgs_behind = max(0, (high - current)) if (high is not None and current is not None) else None

                report["partitions"].append({
                    "topic": tp.topic,
                    "partition": tp.partition,
                    "low": low,
                    "high": high,
                    "position": current,
                    "messages_behind": msgs_behind,
                })
                if msgs_behind is not None:
                    report["total_messages_behind"] += msgs_behind
            except Exception as e:
                logger.warning("lag_status failed for %s[%d]: %s", tp.topic, tp.partition, e)
        return report

    def lag_status(self) -> Future:
        """Return per-partition lag info.
        - `messages_behind`: high_watermark - current_position
        - `older_than_window`: True if current_position is before the offset at (now - window_seconds)
        Note: This provides an approximation using Kafka timestamps and may vary with producer timestamp behavior.
        """
        if not self._subscribed:
            raise RuntimeError("lag_status() called before subscribe()")

        assignment = self.consumer.assignment() or []
        now_ms = int(time.time() * 1000)

        return self._executor.submit(self.get_lag_status_from_kafka_async, assignment)


    # ---- Helpers ------------------------------------------------------------------
    @staticmethod
    def _decode_headers(headers: Optional[List[Tuple[str, bytes]]]) -> Dict[str, Optional[str]]:
        if not headers:
            return {}
        out: Dict[str, Optional[str]] = {}
        for k, v in headers:
            try:
                out[k] = v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else (v if v is None else str(v))
            except Exception:
                out[k] = None
        return out

    @staticmethod
    def _parse_value(value_bytes: Optional[bytes]) -> Any:
        if value_bytes is None:
            return None
        # Try JSON first, fall back to utf-8 string
        try:
            return json.loads(value_bytes.decode("utf-8"))
        except Exception:
            try:
                return value_bytes.decode("utf-8", errors="replace")
            except Exception:
                return value_bytes  # as raw bytes

    def _to_dict(self, msg) -> Dict[str, Any]:
        value = self._parse_value(msg.value())
        key = None
        try:
            if msg.key() is not None:
                key = msg.key().decode("utf-8")
        except Exception:
            key = msg.key()  # leave as bytes if not decodable

        timestamp = msg.timestamp()  # (type, value)
        ts_type, ts_val = (None, None)
        if isinstance(timestamp, tuple) and len(timestamp) == 2:
            ts_type, ts_val = timestamp

        out = {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp_type": ts_type,
            "timestamp": ts_val,
            "key": key,
            "value": value,
            "headers": self._decode_headers(msg.headers()),
            "_raw": msg,
        }
        return out

    # Rebalance callbacks (optional but helpful for logging)
    def _on_assign(self, the_consumer, partitions):
        logger.info(f"Partitions {partitions} assigned for consumer {the_consumer}.")

    def _on_revoke(self, consumer, partitions):
        logger.info("Partitions revoked: %s", partitions)


if __name__ == "__main__":
    # Minimal test harness; do not run an infinite loop here.
    common_kafka_config = {
        "KAFKA_BOOTSTRAP": os.environ.get("KAFKA_BOOTSTRAP"),
        "KAFKA_USER": os.environ.get("KAFKA_USER"),
        "KAFKA_PASSWORD": os.environ.get("KAFKA_PASSWORD"),
        # Optional overrides
        # "KAFKA_GROUP_ID": "ouster2pg_localtest",
        # "KAFKA_AUTO_OFFSET_RESET": "earliest",
        # "KAFKA_ENABLE_AUTO_COMMIT": False,
        # "KAFKA_CA_LOCATION": "strimzi-ca.crt",
    }

    consumer = KafkaConfluentConsumer(common_kafka_config)
    topic = os.environ.get("KAFKA_TOPIC", "my-topic")
    consumer.subscribe([topic])
    print(consumer.lag_status())

    msg = consumer.poll(timeout=2.0)
    if msg:
        print({k: v for k, v in msg.items() if k not in ("_raw", 'value')})
        print({k: v for k, v in msg['value']['object_list'][0].items() if k not in ('objects',)})
        for obj in msg['value']["object_list"][0]["objects"]:
            print('\t', obj)

    partition_lag = {}
    num_messages = {}
    total_messages = 0
    size_messages = {}
    try:
        while True:
            msgs = consumer.consume_batch(max_messages=100, timeout=5.0)
            if msgs:
                for msg in msgs:
                    # print(f"Intersection ID: {msg['key']} (from partition {msg['partition']})")
                    v = msg['value']
                    p = msg['partition']
                    ts_utc = datetime.datetime.fromtimestamp(msg['timestamp'] / 1000, tz=zoneinfo.ZoneInfo("UTC"))
                    ts_local = ts_utc.astimezone(tz=zoneinfo.ZoneInfo('US/Central'))
                    lag = round((datetime.datetime.now(tz=zoneinfo.ZoneInfo('US/Central')) - ts_local).total_seconds(), 3)
                    sz = len(json.dumps(v)) if isinstance(v, (dict, list)) else len(str())
                    partition_lag[p] = lag
                    num_messages[p] = num_messages.get(p, 0) + 1
                    size_messages[p] = round(size_messages.get(p, 0) + sz / 1024, 1)
                    total_messages += 1
                    if total_messages % 1000 == 0:
                        print(consumer.lag_status())
                print(sorted(list(partition_lag.items())), sorted(list(num_messages.items())), sorted(list(size_messages.items())))
    except KeyboardInterrupt:
        print("BREAK")
    finally:
        consumer.close()

    # print("\nAttempting batch consume.\n")
    # t1 = time.time()
    # msgs = consumer.consume_batch(max_messages=100, timeout=5.0)
    # print(f"Received {len(msgs)} messages in {time.time() - t1:.2f} seconds.")
    # consumer.close()
