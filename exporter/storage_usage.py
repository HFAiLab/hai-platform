
"""
    用于收集存储用量信息的 Prometheus exporter.
    针对具体文件系统和具体需求, 实现并注册对应的 collector (参考 `exporter/storage/ceph_collector.py`) 后, 即可被
`storage-usage-exporter` 组件加载. Prometheus 抓取的用量数据持久化至 InfluxDB 后, 可由 `monitor.monitor_data` 中的接口读取并处理.
"""

import sys

import uvicorn
from fastapi import FastAPI
from prometheus_client.core import GaugeMetricFamily, CollectorRegistry

from logm import logger
from storage import collectors
from exporter.exporter_utils import make_scrape_endpoint


class StorageCollector(object):
    def __init__(self, measurement, collect_func, columns, labels):
        self.collect_func = collect_func
        self.measurement = measurement
        self.columns = columns
        self.labels = labels

    def collect(self):
        c = GaugeMetricFamily(self.measurement, f'metrics of {self.measurement}',
                              labels=['type', 'host_path', 'name']+self.labels)
        if (metrics := self.collect_func()) is None or len(metrics) == 0:
            logger.error(f'{self.measurement} 未获取到数据')
        else:
            try:
                for metric in metrics:
                    for name in self.columns:
                        c.add_metric(
                            [self.measurement, metric['host_path'], name] + [metric[label] for label in self.labels],
                            metric[name] if metric.get(name) is not None else 0
                        )
            except Exception as e:  # catch 住防止影响其他 collector
                logger.error(f'collect {self.measurement} 失败! {e}')
                logger.exception(e)
        yield c


app = FastAPI()
collector_registry = CollectorRegistry()
for collector_config in collectors:
    collector_registry.register(StorageCollector(*collector_config))
    print('已加载 collector config:', collector_config)


app.get('/metrics')(make_scrape_endpoint(collector_registry))


if __name__ == "__main__":
    print(f'server started at: 8080, python', sys.version)
    server_module = 'storage_usage:app'
    uvicorn.run(server_module, host="0.0.0.0", port=8080)
