import pika
from airflow.hooks.base import BaseHook


class RabbitMQHook(BaseHook):
    """RabbitMQ interaction hook.

    :param rabbitmq_conn_id: 'Conn ID` of the Connection to be used to
    configure this hook, defaults to default_conn_name
    :type rabbitmq_conn_id: str, optional
    """

    conn_name_attr = "rabbitmq_conn_id"
    default_conn_name = "rabbitmq_default"
    conn_type = "amqp"
    hook_name = "RabbitMQ"

    @staticmethod
    def get_ui_field_behaviour():
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["extra"],
            "relabeling": {"schema": "vhost"},
            "placeholders": {
                "login": "guest",
                "password": "guest",
                "port": "5672",
                "host": "localhost",
                "schema": "/",
            },
        }

    def __init__(
        self,
        rabbitmq_conn_id: str = default_conn_name,
    ) -> None:
        super().__init__()
        self.rabbitmq_conn_id = rabbitmq_conn_id
        self._client = None

    def get_conn(self):
        """Returns connection to RabbitMQ using pika.

        :return: connection to RabbitMQ server
        :rtype: pika.BlockingConnection
        """

        conn = self.get_connection(self.rabbitmq_conn_id)

        credentials = pika.PlainCredentials(conn.login, conn.password)
        if not conn.schema:
            conn.schema = "/"  # if no vhost given, set to /
        parameters = pika.ConnectionParameters(
            conn.host, conn.port, conn.schema, credentials
        )
        connection = pika.BlockingConnection(parameters)
        return connection

    def publish(self, exchange: str, routing_key: str, message: str) -> None:
        """Publish a message.

        :param exchange: the exchange to publish to
        :type exchange: str
        :param routing_key: the routing key to publish to
        :type routing_key: str
        :param message: the message to publish
        :type message: str
        """
        connection = self.get_conn()
        channel = connection.channel()
        channel.basic_publish(exchange, routing_key, message)
        channel.close()

    def declare_queue(self, name: str, passive: bool = False) -> pika.frame.Method:
        """Declare a queue.

        :param name: the queue name
        :type name: str
        :param passive: Only check to see if the queue exists and raise
          `ChannelClosed` if it doesn't, defaults to False
        :type passive: bool, optional
        :return: Method frame
        :rtype: pika.frame.Method
        """
        connection = self.get_conn()
        channel = connection.channel()
        declaration = channel.queue_declare(name, passive)
        channel.close()
        return declaration

    def purge_queue(self, name: str) -> pika.frame.Method:
        """Purge a queue.

        :param name: the queue name
        :type name: str
        :returns: Method frame
        :rtype: pika.frame.Method
        """
        connection = self.get_conn()
        channel = connection.channel()
        return channel.queue_purge(name)

    def delete_queue(self, name: str) -> pika.frame.Method:
        """Delete a queue.

        :param name: the queue name
        :type name: str
        :returns: Method frame
        :rtype: pika.frame.Method
        """
        connection = self.get_conn()
        channel = connection.channel()
        return channel.queue_delete(name)

    def pull(self, queue: str) -> str:
        """Pull and acknowledge a message from the queue.

        :param queue: the queue to pull messages from
        :type queue: str
        :return: the pulled message, if one exists
        :rtype: str
        """
        connection = self.get_conn()
        channel = connection.channel()
        method_frame, _, body = channel.basic_get(queue)
        if method_frame:
            channel.basic_ack(method_frame.delivery_tag)
            message = body
        else:
            message = None
        channel.close()
        return message
