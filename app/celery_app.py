"""Celery's synchronous producer/worker configuration.

This is separate from ``app.redis_client``: the latter is an asyncio Redis
client for live chat, while Celery manages its own broker/result connections.
"""

from celery import Celery

from app.config import settings

celery_app = Celery(
    "chat",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["app.tasks"],
)
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
    result_expires=3600,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    task_publish_retry=False,
    broker_transport_options={
        "global_keyprefix": f"{settings.redis_namespace}:celery:",
        "socket_connect_timeout": 5,
        "socket_timeout": 5,
    },
    redis_socket_connect_timeout=5,
    redis_socket_timeout=5,
    result_backend_transport_options={"global_keyprefix": f"{settings.redis_namespace}:results:"},
)
