import json
import os
import sys
import logging
import time
import datetime
import zoneinfo
from typing import Iterable, List, Optional, Tuple, Dict, Any

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
        auto_offset_reset = config.get("KAFKA_AUTO_OFFSET_RESET", "earliest")
        enable_auto_commit = bool(config.get("KAFKA_ENABLE_AUTO_COMMIT", False))
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
            # Consumer-specific
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": enable_auto_commit,
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

    # ---- Subscription / lifecycle -------------------------------------------------
    def subscribe(self, topics: List[str], initialize_with_poll: bool = True, init_retries: int = 5) -> None:
        if not topics:
            raise ValueError("subscribe() requires at least one topic")
        self.consumer.subscribe(topics, on_assign=self._on_assign, on_revoke=self._on_revoke)
        self._subscribed = True
        logger.info("Subscribed to topics: %s", topics)
        for topic in topics:
            md = self.consumer.list_topics(topic=topic, timeout=10.0)
            partitions = list(md.topics[topic].partitions.keys())  # [0,1,2,...]
            logger.info(f"\tAvailable partitions for topic {topic}: {partitions}")
            topic_partitions = [TopicPartition(topic, p) for p in partitions]
            self.consumer.assign(topic_partitions)
        if initialize_with_poll is True:
            self.initialize_poll(retries=init_retries)

    def close(self) -> None:
        try:
            self.consumer.close()
        except Exception:
            logger.exception("Error closing consumer")

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

        # TODO: return partition offsets so we can keep track of where consumer is, if desired
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

    def maybe_fast_forward(self, max_delay_seconds: Optional[int] = None, reinitialize_with_poll: bool = True) -> None:
        """Ensure current start position is no older than (now - delay). Uses assign() with explicit offsets."""
        if not self._subscribed:
            raise RuntimeError("maybe_fast_forward() called before subscribe()")

        delay = int(max_delay_seconds if max_delay_seconds is not None else self._max_start_delay_seconds)
        if delay < 0:
            raise ValueError("Max delay for fast forward must be >= 0.")

        assignment = self.consumer.assignment() or []
        if not assignment:
            logger.info("maybe_fast_forward(): no partitions currently assigned")
            return

        now_ms = int(time.time() * 1000)
        target_ts = now_ms - (delay * 1000)
        logger.info(f"maybe_fast_forward(): delay={delay} seconds, target Kafka timestamp={target_ts} (ms since epoch)")

        # Ask broker for offsets at target timestamp for each partition
        tps = [TopicPartition(tp.topic, tp.partition, target_ts) for tp in assignment]
        try:
            results = self.consumer.offsets_for_times(tps, timeout=10.0) or []
            logger.info("offsets_for_times() returned results for %d partitions", len(results))

            modified = []
            for idx, tp in enumerate(assignment):
                try:
                    res = results[idx] if idx < len(results) else None
                    if res is None or res.offset is None or res.offset < 0:
                        low, high = self.consumer.get_watermark_offsets(tp, timeout=5.0)
                        target_offset = high
                        logger.info("Partition %s[%d]: no valid ts-offset, using high watermark=%s",
                                    tp.topic, tp.partition, target_offset)
                    else:
                        target_offset = res.offset
                        logger.info("Partition %s[%d]: target offset for ts>=window=%s",
                                    tp.topic, tp.partition, target_offset)

                    # If current position is known and already ahead of target, keep it
                    pos_tp = self.consumer.position([tp])
                    current_offset = pos_tp[0].offset if pos_tp and pos_tp[0] and pos_tp[0].offset is not None else None
                    if current_offset is None or current_offset < 0 or current_offset < target_offset:
                        chosen = target_offset
                        reason = "advance-to-target"
                    else:
                        chosen = current_offset
                        reason = "already-ahead"

                    modified.append(TopicPartition(tp.topic, tp.partition, chosen))
                    logger.info("Partition %s[%d]: choose offset=%s (current=%s, target=%s, reason=%s)",
                                tp.topic, tp.partition, chosen, current_offset, target_offset, reason)
                except Exception as e:
                    logger.warning("Manual fast-forward compute failed for %s[%d]: %s", tp.topic, tp.partition, e)

            if modified:
                self.consumer.assign(modified)
                logger.info("maybe_fast_forward(): applied modified assignment (%d partitions)", len(modified))
                try:
                    # Trigger state update so position() and lag reflect the new assignment
                    self.consumer.poll(0)
                except Exception:
                    pass
        except Exception as e:
            logger.warning("maybe_fast_forward(): offsets_for_times failed: %s", e)

        if reinitialize_with_poll is True:
            self.initialize_poll()

    def lag_status(self, window_seconds: int = 60) -> Dict[str, Any]:
        """Return per-partition lag info.
        - `messages_behind`: high_watermark - current_position
        - `older_than_window`: True if current_position is before the offset at (now - window_seconds)
        Note: This provides an approximation using Kafka timestamps and may vary with producer timestamp behavior.
        """
        if not self._subscribed:
            raise RuntimeError("lag_status() called before subscribe()")

        assignment = self.consumer.assignment() or []
        try:
            self.consumer.poll(0)
        except Exception:
            pass
        now_ms = int(time.time() * 1000)
        target_ts = now_ms - (window_seconds * 1000)

        # Map desired timestamp to offsets
        tps = [TopicPartition(tp.topic, tp.partition, target_ts) for tp in assignment]
        try:
            ts_offsets = self.consumer.offsets_for_times(tps, timeout=10.0) or []
        except Exception:
            ts_offsets = [None] * len(assignment)

        report = {"partitions": [], "total_messages_behind": 0}

        for idx, tp in enumerate(assignment):
            try:
                low, high = self.consumer.get_watermark_offsets(tp, timeout=5.0)
                pos_tp = self.consumer.position([tp])
                current = pos_tp[0].offset if pos_tp and pos_tp[0] and pos_tp[0].offset is not None else low
                msgs_behind = max(0, (high - current)) if (high is not None and current is not None) else None

                target_off = None
                res = ts_offsets[idx] if idx < len(ts_offsets) else None
                if res is not None and res.offset is not None and res.offset >= 0:
                    target_off = res.offset
                older_than_window = (current is not None and target_off is not None and current < target_off)

                report["partitions"].append({
                    "topic": tp.topic,
                    "partition": tp.partition,
                    "low": low,
                    "high": high,
                    "position": current,
                    "messages_behind": msgs_behind,
                    "older_than_window": older_than_window,
                })
                if msgs_behind is not None:
                    report["total_messages_behind"] += msgs_behind
            except Exception as e:
                logger.warning("lag_status failed for %s[%d]: %s", tp.topic, tp.partition, e)
        return report

    # ---- Offsets ------------------------------------------------------------------
    def commit(self, msg_or_raw: Any = None, asynchronous: bool = False) -> None:
        """
        Commit offsets after successful downstream processing.
        - If msg_or_raw is the dict returned by poll()/consume_batch, we use msg_or_raw["_raw"].
        - If it's already a confluent_kafka.Message, we use it directly.
        - If None, performs a synchronous/async commit of the current position.
        """
        try:
            if msg_or_raw is None:
                self.consumer.commit(asynchronous=asynchronous)
            else:
                raw = msg_or_raw
                if isinstance(msg_or_raw, dict) and "_raw" in msg_or_raw:
                    raw = msg_or_raw["_raw"]
                self.consumer.commit(message=raw, asynchronous=asynchronous)
        except KafkaException as e:
            logger.error("Kafka commit exception: %s", e)

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
    def _on_assign(self, consumer, partitions):
        logger.info("Partitions assigned: %s", partitions)
        # Rebalance-safe fast-forward: use assign() with explicit offsets
        try:
            if self._max_start_delay_seconds and self._max_start_delay_seconds > 0 and partitions:
                now_ms = int(time.time() * 1000)
                target_ts = now_ms - (self._max_start_delay_seconds * 1000)
                tps = [TopicPartition(p.topic, p.partition, target_ts) for p in partitions]
                results = consumer.offsets_for_times(tps, timeout=10.0) or []

                modified = []
                for idx, p in enumerate(partitions):
                    try:
                        res = results[idx] if idx < len(results) else None
                        if res is None or res.offset is None or res.offset < 0:
                            low, high = consumer.get_watermark_offsets(p, timeout=5.0)
                            target_offset = high  # start at end if no ts match
                            reason = "end (no timestamp match)"
                        else:
                            target_offset = res.offset
                            reason = f"ts>={self._max_start_delay_seconds}s"

                        tp_target = TopicPartition(p.topic, p.partition, target_offset)
                        modified.append(tp_target)
                        logger.info("Rebalance assign %s[%d] -> offset %s based on %s",
                                    p.topic, p.partition, target_offset, reason)
                    except Exception as e:
                        logger.warning("Failed to compute target offset for %s[%d]: %s", p.topic, p.partition, e)

                if modified:
                    consumer.assign(modified)
                    logger.info("Applied modified assignment with %d partitions", len(modified))
                else:
                    consumer.assign(partitions)
            else:
                consumer.assign(partitions)
        except Exception as e:
            logger.warning("on_assign fast-forward check failed: %s", e)

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

    print("\nFast forward to recent.\n")
    consumer.maybe_fast_forward(max_delay_seconds=30)

    partition_lag = {}
    num_messages = {}
    size_messages = {}
    try:
        while True:
            msg = consumer.poll(timeout=2.0)
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
                    # print(f"Timestamp: {ts_local}")
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
