import pika
import json
import time
import logging
from config import *
from config_logger import configurar_logger
from pika.exchange_type import ExchangeType

logger  = configurar_logger( name   = f"publisher", 
                            log_dir = './log',
                            nivel   = logging.INFO)

class ExampleSyncPublisher:
    EXCHANGE        = EXCHANGENAME
    EXCHANGE_TYPE   = ExchangeType.fanout 
    ROUTING_KEY     = ''        
    EXCHANGE_DELAYED      = 'delayed_exchange'
    EXCHANGE_TYPE_DELAYED = 'x-delayed-message'
    ARGUMENTS_DELAYED     = {'x-delayed-type': 'direct'}
    HEADERS_DELAYED       = {'x-delay': REDIS_TIME}
    ROUTING_KEY_DELAYED   = 'delete_frame'

    def __init__(self, amqp_url, max_retries=2, retry_delay=1):
        self._url = amqp_url
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._connection  = None
        self._channel     = None
        self.connect()

    def connect(self):
        """Establece una conexión y canal, con reintentos si es necesario."""
        for attempt in range(1, self._max_retries + 1):
            try:
                parameters = pika.URLParameters(self._url)
                self._connection = pika.BlockingConnection(parameters)
                self._channel = self._connection.channel()
                self.setup_exchanges()
                logger.info("Conexión establecida a RabbitMQ")
                return
            except Exception as e:
                logger.error("Intento %d/%d de conectar falló: %s", attempt, self._max_retries, e)
                time.sleep(self._retery_delay)
        raise Exception("No se pudo conectar a RabbitMQ tras varios intentos.")

    def setup_exchanges(self):
        """Declara el exchange normal y el exchange delayed."""
        # Exchange normal
        self._channel.exchange_declare(
            exchange        = self.EXCHANGE,
            exchange_type   = self.EXCHANGE_TYPE,
            durable         = True
        )
        # Exchange delayed: de tipo x-delayed-message con comportamiento 'direct'
        self._channel.exchange_declare(
            exchange        = self.EXCHANGE_DELAYED,
            exchange_type   = self.EXCHANGE_TYPE_DELAYED,
            arguments       = self.ARGUMENTS_DELAYED
        )

    def reconnect(self):
        """Cierra la conexión actual (si existe) e intenta reconectar."""
        try:
            if self._connection:
                self._connection.close()
        except Exception:
            pass
        self.connect()

    def publish(self, message, exchange=None, routing_key=None, headers=None):
        """Publica un mensaje en el exchange normal de forma síncrona."""
        body        = json.dumps(message)
        properties  = pika.BasicProperties(delivery_mode=2)
        for attempt in range(1, self._max_retries + 1):
            try:
                self._channel.basic_publish(
                    exchange    = self.EXCHANGE,
                    routing_key = self.ROUTING_KEY,
                    body        = body,
                    properties  = properties
                )
                logger.info("Mensaje publicado en '%s' con routing key '%s'", exchange, routing_key)
                return
            except Exception as e:
                logger.error("Error al publicar mensaje (intento %d): %s", attempt, e)
                self.reconnect()
                time.sleep(self._retry_delay)
        logger.error("No se pudo publicar el mensaje tras %d intentos", self._max_retries)

    def publish_delayed(self, message, delay_ms, exchange="delayed_exchange", routing_key="delete_frame"):
        """Publica un mensaje con retraso especificado en milisegundos."""
        body        = json.dumps(message)
        properties  = pika.BasicProperties(headers=self.HEADERS_DELAYED)
        for attempt in range(1, self._max_retries + 1):
            try:
                self._channel.basic_publish(
                    exchange    = self.EXCHANGE_DELAYED,
                    routing_key = self.ROUTING_KEY_DELAYED,
                    body        = body,
                    properties  = properties
                )
                logger.info("Mensaje retrasado publicado con %d ms de delay", delay_ms)
                return
            except Exception as e:
                logger.error("Error al publicar mensaje retrasado (intento %d): %s", attempt, e)
                self.reconnect()
                time.sleep(self._retry_delay)
        logger.error("No se pudo publicar el mensaje retrasado tras %d intentos", self._max_retries)

    def close(self):
        """Cierra la conexión a RabbitMQ."""
        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
                logger.info("Conexión cerrada.")
        except Exception as e:
            logger.error("Error al cerrar la conexión: %s", e)


if __name__ == "__main__":
    AMQP_URL = "amqp://root:winempresas@10.23.63.61:5672/%2F"
    publisher = ExampleSyncPublisher(AMQP_URL)

    try:
        # Publicar mensajes normales
        for i in range(5):
            publisher.publish({"msg": f"Mensaje normal {i}"})
            time.sleep(1)
        # Publicar mensajes con retraso (por ejemplo, 12 segundos)
        for i in range(5):
            publisher.publish_delayed({"msg": f"Mensaje retrasado {i}"}, delay_ms=4000)
            time.sleep(1)
    finally:
        publisher.close()
