import pytest
from pika.exceptions import ChannelClosedByBroker

from rabbitmq_provider.hooks.rabbitmq import RabbitMQHook


@pytest.fixture
def rabbitmq_hook(monkeypatch):
    monkeypatch.setenv(
        "AIRFLOW_CONN_CONN_RABBITMQ", "amqp://guest:guest@localhost:5672"
    )
    return RabbitMQHook(rabbitmq_conn_id="conn_rabbitmq")


@pytest.fixture(autouse=True)
def delete_test_queue(rabbitmq_hook):
    rabbitmq_hook.delete_queue("test")


def test_hook(monkeypatch):
    monkeypatch.setenv(
        "AIRFLOW_CONN_CONN_RABBITMQ", "amqp://guest:guest@localhost:5672"
    )
    hook = RabbitMQHook(rabbitmq_conn_id="conn_rabbitmq")
    assert hook.get_conn()


def test_hook_publish_and_pull(rabbitmq_hook):
    rabbitmq_hook.declare_queue("test")
    rabbitmq_hook.publish("", "test", "Hello World")
    message = rabbitmq_hook.pull("test")
    assert message == b"Hello World"


def test_hook_queue_declare(rabbitmq_hook):
    rabbitmq_hook.declare_queue("test")
    queue = rabbitmq_hook.declare_queue("test", passive=True)
    assert queue


def test_hook_queue_purge(rabbitmq_hook):
    rabbitmq_hook.declare_queue("test")
    rabbitmq_hook.publish("", "test", "Hello World")
    rabbitmq_hook.purge_queue("test")
    message = rabbitmq_hook.pull("test")
    assert message is None


def test_hook_queue_delete(rabbitmq_hook):
    rabbitmq_hook.declare_queue("to_be_deleted")
    rabbitmq_hook.delete_queue("to_be_deleted")
    with pytest.raises(
        ChannelClosedByBroker,
        match="""\(404, "NOT_FOUND - no queue \'to_be_deleted\'""",
    ):
        rabbitmq_hook.declare_queue("to_be_deleted", passive=True)
