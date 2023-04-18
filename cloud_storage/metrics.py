
from prometheus_client import Counter, Gauge


RUNNING_TASKS_GAUGE = Gauge(
    "cloud_storage_tasks_running",
    "Running tasks of current cloud storage server.",
    labelnames=("direction", "user", "file_type",)
)

Failed_TASKS_COUNTER = Counter(
    "cloud_storage_tasks_failed_total",
    "Failed tasks of current cloud storage server.",
    labelnames=("direction", "user", "file_type",)
)

# 只统计pull
SYNCING_FILESIZE_GAUGE = Gauge(
    "cloud_storage_syncing_file_size",
    "Syncing file size of current cloud storage server.",
    labelnames=("direction", "user", "file_type",)
)

SYNCED_FILESIZE_COUNTER = Counter(
    "cloud_storage_synced_file_size_total",
    "Synced file size of current cloud storage server.",
    labelnames=("direction", "user", "file_type",)
)

SYNCED_FILENUM_COUNTER = Counter(
    "cloud_storage_synced_file_num_total",
    "Synced file num of current cloud storage server.",
    labelnames=("direction", "user", "file_type",)
)

DB_FAILURE_COUNTER = Counter(
    "cloud_storage_db_failure_total",
    "db failure of current cloud storage server.",
    labelnames=("operation",)
)

BUCKET_USAGE_SIZE = Gauge(
    "cloud_storage_bucket_usage_size",
    "cloud storage bucket usage.",
    labelnames=("group", "user", "file_type",)
)
