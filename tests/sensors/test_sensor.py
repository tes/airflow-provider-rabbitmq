import pytest
import pika

from airflow.exceptions import AirflowSensorTimeout
from rabbitmq_provider.hooks.rabbitmq import RabbitMQHook
from rabbitmq_provider.sensors.rabbitmq import RabbitMQSensor


@pytest.fixture(autouse=True)
def reset_test_queue(monkeypatch):
    monkeypatch.setenv(
        "AIRFLOW_CONN_CONN_RABBITMQ", "amqp://guest:guest@localhost:5672"
    )
    hook = RabbitMQHook(rabbitmq_conn_id="conn_rabbitmq")
    hook.delete_queue("test")
    hook.declare_queue("test")


def test_sensor():
    sensor = RabbitMQSensor(
        task_id="sample_sensor_check",
        rabbitmq_conn_id="conn_rabbitmq",
        queue_name="test",
    )
    hook = RabbitMQHook(rabbitmq_conn_id="conn_rabbitmq")
    hook.publish("", "test", "Hello World")

    # Queue has 1 message, will be consumed and return True
    assert sensor.poke(context={}) is True
    # Queue now has no message, will return False
    assert sensor.poke(context={}) is False


def test_sensor_with_empty_content():
    sensor = RabbitMQSensor(
        task_id="sample_sensor_check",
        rabbitmq_conn_id="conn_rabbitmq",
        queue_name="test",
    )
    hook = RabbitMQHook(rabbitmq_conn_id="conn_rabbitmq")
    hook.publish("", "test", "")

    # Queue has 1 message, will be consumed and return True
    assert sensor.poke(context={}) is True
    # Queue now has no message, will return False
    assert sensor.poke(context={}) is False


def test_sensor_execute_returns_message():
    sensor = RabbitMQSensor(
        task_id="sample_sensor_check",
        rabbitmq_conn_id="conn_rabbitmq",
        queue_name="test",
        poke_interval=1,
        timeout=2,
    )
    hook = RabbitMQHook(rabbitmq_conn_id="conn_rabbitmq")
    hook.publish("", "test", "Hello World")

    # Queue has 1 message, will be consumed and return the message
    assert sensor.execute(context={}) == "Hello World"

    # Queue now has no message
    # Execute will timeout (it goes until it gets a True poke)
    with pytest.raises(AirflowSensorTimeout):
        sensor.execute(context={})
    # Poke will return False as no message
    assert sensor.poke(context={}) is False


def test_errors_if_queue_does_not_exist():
    with pytest.raises(
        pika.exceptions.ChannelClosedByBroker,
        match="""\(404, \"NOT_FOUND - no queue \'does_not_exist\' in vhost""",
    ):
        sensor = RabbitMQSensor(
            task_id="sample_sensor_check",
            rabbitmq_conn_id="conn_rabbitmq",
            queue_name="does_not_exist",
            poke_interval=1,
            timeout=2,
        )
        sensor.execute(context={})
