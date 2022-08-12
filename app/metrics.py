from prometheus_client.metrics import Counter

from app.config import StorageType


class BootstrapMetrics:
    events_count = Counter(
        "bootstrap_count_event_in_bucket",
        "Количество сгенерированных событий по каждому бакету",
        labelnames=["bucket"],
    )

    minio_requests = Counter(
        "minio_requests",
        "Кол-во запросов к минио",
        labelnames=["requests_to_minio"],
    )
    obs_requests = Counter(
        "obs_requests",
        "Кол-во запросов к сберу",
        labelnames=["requests_to_obs"],
    )

    @staticmethod
    def collect_events_count_bucket_metrics(bucket_name: str) -> None:
        BootstrapMetrics.events_count.labels(bucket=bucket_name).inc(1)

    @staticmethod
    def collect_requests_metrics(storage_type: str) -> None:
        if storage_type == StorageType.minio:
            BootstrapMetrics.minio_requests.labels("requests_to_minio").inc(1)
        elif storage_type == StorageType.obs:
            BootstrapMetrics.obs_requests.labels("requests_to_obs").inc(1)
