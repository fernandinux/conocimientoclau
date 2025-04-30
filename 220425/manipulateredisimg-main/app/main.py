import os
import json
import cv2  # type: ignore
import time
from datetime import datetime
import logging
import threading
import redis # type: ignore
import numpy as np # type: ignore

from execute import *
# Importamos la configuración, el consumer y el publicador
from config import *
from config_logger import configurar_logger
from consumer import ReconnectingExampleConsumer
#from publisher import ExamplePublisher

logger = configurar_logger( name    = f"Main", 
                            log_dir = FILE_PATH_LOG,
                            nivel   = logging.DEBUG)




# Pool de conexiones global para Redis
redis_pool = redis.ConnectionPool(
    host=HOST_REDIS, 
    port=6379, 
    db=0,
    socket_timeout=5
)

# r = redis.Redis(host=HOST_REDIS, port=PORT_REDIS, db=0, socket_timeout=5,
#             decode_responses=False ) # Mantener datos como bytes

def rabbitmq_message_callback(channel, basic_deliver, properties, body):
    """
    Callback para RabbitMQ. Si el mensaje indica un frame candidato, se lanza un thread
    que recupera la imagen de Redis y la encola para su procesamiento.
    """
    try:
        data = json.loads(body.decode('utf-8'))
        logger.info(f"Datos recibidos: {data}")
        # candidate = data[DATA_CANDIDATO]
        cam_id = data.get(NAME_CAMERA)
        epoch_id = data.get(NAME_EPOCH)

        candidate=1
        if candidate == 1:
            thread = threading.Thread(target=getImg, args=(cam_id,epoch_id))
            thread.start()
            logger.info(f"Mensaje recibido para cámara {cam_id}: {data} - Procesando en thread")
        else:
            logger.info(f"Mensaje recibido para cámara {cam_id}: {data} - No es frame candidato, no se procesa")
    except Exception as e:
        logger.exception("Error processing RabbitMQ message")

def execute():
    """
    Función principal: inicializa el pool de workers, el thread de la cola y el consumidor RabbitMQ.
    """
    # global pool
    # mp.set_start_method("spawn", force=True)
    # pool = Pool(processes=GPU_WORKERS, initializer=init_worker)
    
    # queue_thread = threading.Thread(target=process_image_queue, daemon=True)
    # queue_thread.start()
    
    consumer = ReconnectingExampleConsumer(AMQP_URL, message_callback=rabbitmq_message_callback)
    try:
        logger.info("Iniciando consumer RabbitMQ...")
        consumer.run()
    except KeyboardInterrupt:
        logger.info("Interrupción por teclado. Cerrando...")


if __name__ == "__main__":
    logger.info("Inicio de programa Main hilos")
    execute()
