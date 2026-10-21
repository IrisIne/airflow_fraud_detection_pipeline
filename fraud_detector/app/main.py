"""Consume transactions, score them and publish versioned results to Kafka."""

from __future__ import annotations

import json
import logging
import signal
from uuid import UUID

from confluent_kafka import Consumer, KafkaError, Producer

from common.features import FeatureValidationError
from .config import Settings
from .model_registry import HotReloadScorer, ModelRegistryError


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger("fraud_detector")


def run() -> None:
    settings = Settings()
    scorer = HotReloadScorer(settings.model_dir, settings.fraud_threshold)
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    consumer.subscribe([settings.transactions_topic])
    running = True

    def stop(_signum, _frame) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    LOGGER.info("scoring started with model_version=%s", scorer.version)
    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    LOGGER.error("Kafka consumer error: %s", message.error())
                continue
            try:
                payload = json.loads(message.value().decode("utf-8"))
                transaction_id = str(UUID(str(payload["transaction_id"])))
                transaction = payload["data"]
                if not isinstance(transaction, dict):
                    raise ValueError("data must be an object")
                score, fraud_flag, model_version = scorer.predict(transaction)
                result = {
                    "transaction_id": transaction_id,
                    "score": score,
                    "fraud_flag": fraud_flag,
                    "model_version": model_version,
                }
                producer.produce(
                    settings.scores_topic,
                    key=transaction_id,
                    value=json.dumps(result).encode("utf-8"),
                )
                producer.flush(10)
                consumer.commit(message=message, asynchronous=False)
                LOGGER.info(
                    "scored transaction_id=%s score=%.6f model_version=%s",
                    transaction_id,
                    score,
                    model_version,
                )
            except (json.JSONDecodeError, UnicodeDecodeError, KeyError, TypeError, ValueError, FeatureValidationError):
                LOGGER.exception("invalid transaction; message skipped")
                consumer.commit(message=message, asynchronous=False)
            except ModelRegistryError:
                LOGGER.exception("model reload failed; offset remains uncommitted")
    finally:
        producer.flush(10)
        consumer.close()


if __name__ == "__main__":
    run()
