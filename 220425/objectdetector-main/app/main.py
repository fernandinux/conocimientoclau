import os
import torch.multiprocessing as mp  # type: ignore
from torch.multiprocessing import Pool, current_process  # type: ignore
import torch  # type: ignore
import json
import cv2  # type: ignore
import time
from datetime import datetime
from ultralytics import YOLO  # type: ignore
import logging
import threading
import queue
import redis
import numpy as np

# Importamos la configuración, el consumer y el publicador
from config import *
from config_logger import configurar_logger
from consumer import ReconnectingExampleConsumer
from publisher import ExamplePublisher

logger = configurar_logger(name=NAME_LOGS, log_dir=LOG_DIR, nivel=LOGGING_CONFIG)

publisher_instance = None 
yolo_instance = None
pool = None

# Cola global para almacenar imágenes y sus datos (con tamaño máximo definido)
# image_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
image_queue = queue.Queue()

# Pool de conexiones global para Redis
redis_pool = redis.ConnectionPool(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    db=REDIS_DB,
    socket_timeout=REDIS_TIMEOUT
)

redis_pool_parameters = redis.ConnectionPool(
    host='10.23.63.68', 
    port=REDIS_PORT, 
    db=REDIS_DB,
    socket_timeout=REDIS_TIMEOUT,
    decode_responses=True 
)

def init_worker():
    global yolo_instance, publisher_instance
    try:
        worker_name = current_process().name
        logger.info(f"🔄 Inicializando modelo en el worker {worker_name}...")
        yolo_instance = YOLO(YOLO_MODEL).to("cuda")
        
        publisher_instance = ExamplePublisher(AMQP_URL)
        publisher_instance.start()
        
        logger.info(f"✅ Modelo cargado en el worker {worker_name}.")
    except Exception as e:
        logger.exception("Error inicializando worker")
        raise

def getCategories(class_dict, selected_categories=("vehicles",)):
    cat_sel = {cat: class_dict.get(cat, []) for cat in selected_categories}
    cat_sel['total'] = [cls for sub in cat_sel.values() for cls in sub]
    return cat_sel

def getResultObjects(model, frame, classes, conf, verbose=False):
    return model.predict(source=frame, classes=classes, conf=conf, verbose=verbose)

def getDate(epoch):
    dt = datetime.fromtimestamp(epoch)
    return dt.strftime(FILE_DATE), dt.strftime(FILE_HOUR)

def saveImg(frame, epoch, cam_id):
    """Guarda la imagen sin bloquear el event loop."""
    inicio_tiempo = time.time()
    day, hour = getDate(epoch / EPOCH_FORMAT)
    output_path = os.path.join("/", FILE_OUTPUT, str(cam_id), day, hour, FILE_FRAME)
    
    os.makedirs(output_path, exist_ok=True)
    img_path = os.path.join(output_path, f"{epoch}.{FILE_FORMAT}")
    
    try:
        if not cv2.imwrite(img_path, frame):
            raise IOError("No se pudo guardar la imagen.")
        elapsed = time.time() - inicio_tiempo
        logger.info(f"Tiempo de escritura en disco: {elapsed:.3f}s")
    except IOError as e:
        logger.error(f"{LOG_SAVE_IMG}: {e}")

def detect_objects_from_image(img, classes, data):
    """
    Procesa la imagen en memoria, ejecuta la detección con YOLO y publica el resultado.
    """
    try:
        timestamp = int(time.time() * EPOCH_FORMAT)
        data[DATA_INIT_TIME] = timestamp
        
        results_yolo = getResultObjects(yolo_instance, img, classes=classes['total'], conf=CONF)
        data[DATA_N_OBJECTOS] = len(results_yolo[0].boxes)
        
        objects_data_list = {}
        for box in results_yolo[0].boxes:
            cat = int(box.cls[0] + 1)
            objects_data_list.setdefault(cat, []).append({
                DATA_OBJETO_COORDENADAS: list(map(int, box.xyxy[0])),
                DATA_OBJETO_PRECISION: int(box.conf[0].item() * 100),
                DATA_OBJETO_EPOCH_OBJECT: int(time.time() * EPOCH_OBJECT_FORMAT)
            })
        data[DATA_OBJETOS_DICT] = objects_data_list
        data[DATA_STATUS_FRAME] = True
        data[DATA_FINAL_TIME] = int(time.time() * EPOCH_FORMAT)
        
        logger.debug(f"Imagen {data.get(data[DATA_EPOCH_FRAME], 'unknown')}: {sum(len(v) for v in objects_data_list.values())} objetos detectados.")
        logger.info(f"Publicando resultado: {data}")
        publisher_instance.publish_async(data)
        return True
    except Exception as e:
        logger.exception("Error en procesamiento de imagen")
        return None

def read_image_and_enqueue(data):
    """
    Conecta a Redis, recupera y decodifica la imagen, y la encola junto con sus datos.
    """
    try:
        inicio_tiempo = time.time()
        r = redis.Redis(connection_pool=redis_pool, decode_responses=False)

        r_parameters = redis.Redis(connection_pool=redis_pool_parameters, decode_responses=True)
        parameters_extra = r_parameters.hgetall(data[DATA_CAMERA])
        parameters_extra = {k: json.loads(v) for k, v in parameters_extra.items()}
        if parameters_extra:
            data['zone_restricted'] = parameters_extra

        image_bytes = r.get(f"{data[DATA_CAMERA]}{data[DATA_EPOCH_FRAME]}") #r.hget(data[DATA_CAMERA], data[DATA_EPOCH_FRAME])
        if not image_bytes:
            logger.error(f"Frame {data[DATA_EPOCH_FRAME]} no existe en Redis")
            return
        
        logger.info(f"Tiempo obtención Redis: {time.time() - inicio_tiempo:.3f}s")
        
        try:
            np_arr = np.frombuffer(image_bytes, dtype=np.uint8)
            logger.info(f"Tiempo buffer to array: {time.time() - inicio_tiempo:.3f}s")
            img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
            logger.info(f"Tiempo array to img: {time.time() - inicio_tiempo:.3f}s")
            
            if img is None:
                logger.error("Formato de imagen inválido")
                return
        except Exception as e:
            logger.exception("Error decodificando imagen")
            return
        
        logger.info(f"Imdecode. Dimensiones: {img.shape} - Tiempo total: {time.time() - inicio_tiempo:.3f}s")
        
        if image_queue.full():
            logger.warning(f"La cola está llena (máximo {MAX_QUEUE_SIZE} elementos). Se descarta la imagen: {data[DATA_EPOCH_FRAME]}")
            return
        
        image_queue.put((img, data))
        logger.info(f"Imagen cargada y almacenada en la cola. Tamaño actual: {image_queue.qsize()}/{MAX_QUEUE_SIZE}")
    except Exception as e:
        logger.exception("Error al abrir imagen")

def process_image_queue():
    """
    Thread que monitoriza la cola y despacha cada imagen al pool de procesos YOLO.
    """
    while True:
        try:
            img, data = image_queue.get()
            classes = getCategories(CLASS_DICT)
            logger.info("Despachando imagen en cola para procesamiento YOLO")
            pool.apply_async(detect_objects_from_image, args=(img, classes, data))
            image_queue.task_done()
        except Exception as e:
            logger.exception("Error procesando imagen de la cola")

def rabbitmq_message_callback(channel, basic_deliver, properties, body):
    """
    Callback para RabbitMQ. Si el mensaje indica un frame candidato, se lanza un thread
    que recupera la imagen de Redis y la encola para su procesamiento.
    """
    try:
        data = json.loads(body.decode('utf-8'))
        logger.info(f"Datos recibidos: {data}")
        candidate = data[DATA_CANDIDATO]
        cam_id = data.get(DATA_CAMERA)
        
        if candidate == 1:
            thread = threading.Thread(target=read_image_and_enqueue, args=(data,))
            thread.start()
            logger.info(f"Mensaje recibido para cámara {cam_id}: {data} - Procesando en thread")
        else:
            logger.info(f"Mensaje recibido para cámara {cam_id}: {data} - No es frame candidato, no se procesa")
    except Exception as e:
        logger.exception("Error processing RabbitMQ message")

def main():
    """
    Función principal: inicializa el pool de workers, el thread de la cola y el consumidor RabbitMQ.
    """
    global pool
    mp.set_start_method("spawn", force=True)
    pool = Pool(processes=GPU_WORKERS, initializer=init_worker)
    
    queue_thread = threading.Thread(target=process_image_queue, daemon=True)
    queue_thread.start()
    
    consumer = ReconnectingExampleConsumer(AMQP_URL, message_callback=rabbitmq_message_callback)
    try:
        logger.info("Iniciando consumer RabbitMQ...")
        consumer.run()
    except KeyboardInterrupt:
        logger.info("Interrupción por teclado. Cerrando...")
    finally:
        pool.close()
        pool.join()
        if publisher_instance is not None:
            publisher_instance.stop()

if __name__ == "__main__":
    logger.info("Inicio de programa")
    main()
