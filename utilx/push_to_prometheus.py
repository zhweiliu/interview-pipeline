from prometheus_client import CollectorRegistry, push_to_gateway, Gauge
import os
from dotenv import load_dotenv
load_dotenv()

def guage(metrics_name: str, value: float):
    PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "localhost:9091")
    registry = CollectorRegistry()
    g = Gauge(
        metrics_name,
        documentation=metrics_name,
        registry=registry
    )
    g.set(value)
    push_to_gateway(PUSHGATEWAY_URL, job=metrics_name, registry=registry)
