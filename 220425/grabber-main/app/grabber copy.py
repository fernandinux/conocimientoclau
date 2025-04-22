import os
import time
import logging
import asyncio
import json
import cv2          # type: ignore
import numpy as np  # type: ignore
import pika         # type: ignore
import asyncio
import aio_pika

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from config_logger import configurar_logger
from publisher import ExamplePublisher
from config import *

logger = configurar_logger( name    = f"{FILE_NAME_LOG}_{os.environ.get(CAMARAID)}", 
                            log_dir = FILE_PATH_LOG,
                            nivel   = logging.WARNING)

# Configuración de concurrencia
executor = ThreadPoolExecutor(max_workers=2)  
queue = asyncio.Queue(maxsize=20)  

def getFlagProccessed(frame1, 
                      frame2, 
                      umbral  = UMBRAL_COUNT_PIXELS, 
                      umbral2 = UMBRAL_REST_PIXEL):
    try:
        if frame2 is None:
            return True
        _, thresh = cv2.threshold(
            cv2.absdiff(cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY), cv2.cvtColor(frame2, cv2.COLOR_BGR2GRAY)),
            umbral2, 255, cv2.THRESH_BINARY)
        return True if np.count_nonzero(thresh) > umbral else False
    except Exception as e:
        logger.error(f"{LOG_FUNC_FLAG}: {e}")
        return False

def getDate(epoch):
    dt = datetime.fromtimestamp(epoch)
    return dt.strftime(FILE_DATE), dt.strftime(FILE_HOUR)

def createFiles(output_path):
    os.makedirs(output_path, exist_ok=True)
    path_parts = output_path.split("/")
    current_path = "/"

    for part in path_parts:
        if part:
            current_path = os.path.join(current_path, part)
            os.chmod(current_path, 0o777)  

# def saveImg(frame, epoch):  
#     day, hour = getDate(epoch/EPOCH_FORMAT)
#     output_path = f"/{FILE_OUTPUT}/{generateVideos.cam_id}/{day}/{hour}/{FILE_FRAME}"

#     createFiles(output_path)

#     try:
#         if not cv2.imwrite(f"{output_path}/{epoch}.{FILE_FORMAT}", frame):
#             raise IOError("No se pudo guardar la imagen.")
#     except IOError as e:
#         logger.error(f"{LOG_SAVE_IMG}: {e}")

# def send_message(channel, json_data, exchange):
#     try:
#         json_string = json.dumps(json_data)
#         channel.basic_publish(
#             exchange    = exchange,
#             routing_key = EXCHANGE_ROUTING, 
#             body        = json_string,
#             properties  = pika.BasicProperties(
#                 delivery_mode   = EXCHANGE_DELMODE
#             )
#         )

#     except Exception as e:
#         logger.exception(f"{LOG_PUBLISHER}: {e}")

def saveImg(frame, epoch, cam_id):
    """Guarda la imagen de manera asíncrona sin bloquear el evento loop"""
    day, hour = getDate(epoch / EPOCH_FORMAT)
    output_path = f"/{FILE_OUTPUT}/{cam_id}/{day}/{hour}/{FILE_FRAME}"
    
    os.makedirs(output_path, exist_ok=True)
    path_parts = output_path.split("/")
    current_path = "/"

    for part in path_parts:
        if part:
            current_path = os.path.join(current_path, part)
            os.chmod(current_path, 0o777)  

    loop = asyncio.get_running_loop()
    try:
        result = loop.run_in_executor(executor, cv2.imwrite, f"{output_path}/{epoch}.{FILE_FORMAT}", frame)
        if not result:
            raise IOError("No se pudo guardar la imagen.")
    except IOError as e:
        logger.error(f"{LOG_SAVE_IMG}: {e}")

def send_message(channel, json_data, exchange_name):
    try:
        json_string = json.dumps(json_data)
        exchange = channel.get_exchange(exchange_name)
        exchange.publish(
            aio_pika.Message(
                body=json_string.encode(),
                delivery_mode=EXCHANGE_DELMODE
            ),
            routing_key=EXCHANGE_ROUTING
        )
    except Exception as e:
        logger.exception(f"{LOG_PUBLISHER}: {e}")


def process_queue(channel, exchange):
    while True:
        frame, data = queue.get()  
        try:
            saveImg(frame, data[NAME_EPOCH])
            send_message(channel, data, exchange)
        finally:
            queue.task_done()  

def producer(frame, data):
    logger.warning(f'el tamaño de la cola es: {queue.qsize}')
    if queue.full():  
        try:
            queue.get_nowait()  
            queue.task_done()  
        except asyncio.QueueEmpty: pass 
    queue.put((frame, data))  # Agregar nuevo frame

# Clase principal
class generateVideos():
    cam_id = (os.environ.get(CAMARAID))
    def __init__(self):
        self.camera_url     = os.environ.get(URL)
        self.camera_id      = os.environ.get(CAMARAID)
        self.fps_deseado    = int(os.environ.get(FPS))
        self.size           = float(os.environ.get(SIZE))

        self.queuehost      = os.environ.get(QUEUEHOST)
        self.queueport      = int(os.environ.get(QUEUEPORT))
        self.queueuser      = os.environ.get(QUEUEUSER)
        self.queuepass      = os.environ.get(QUEUEPASS)
        self.exchangename   = os.environ.get(EXCHANGENAME)
        self.exchangerror   = os.environ.get(EXCHANGERROR)

        self.connection     = None
        self.channel        = None
        self.last_frame     = None   
        self.frame          = None
        if self.queuehost  != "0.0.0.0" : self.connect()

        workers = [asyncio.create_task(process_queue(self.channel)) for _ in range(3)]

        # self.publisher = ExamplePublisher(AMQP_URL)
        # self.publisher.start()

        self.interval_time  = 1 / self.fps_deseado
        self.fps_processed  = [round((i + 1) * self.fps_deseado / PROCESSED_FRAME) for i in range(PROCESSED_FRAME)]
        self.data           = {NAME_CAMERA : self.camera_id, NAME_FUNC : [], NAME_STATUS : False}

        logger.info(f"{LOG_CONFIG_CAM} {self.camera_id}, URL: {self.camera_url}, FPS: {self.fps_deseado}, SIZE: {self.size}")

    def connect(self):
        """Conectar a RabbitMQ con credenciales y declarar el exchange"""
        try:
            credentials = pika.PlainCredentials(self.queueuser, self.queuepass)
            parameters  = pika.ConnectionParameters(
                host        = self.queuehost,
                port        = self.queueport,
                credentials = credentials
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel    = self.connection.channel()

            # Declarar el exchange tipo fanout
            self.channel.exchange_declare(exchange = self.exchangename, exchange_type = EXCHANGE_TYPE, durable = EXCHANGE_DURABLE)
            self.channel.exchange_declare(exchange = self.exchangerror, exchange_type = EXCHANGE_TYPE, durable = EXCHANGE_DURABLE)

            logger.info(f"{LOG_CONFIG_CAM} USER: {self.queueuser}")
        except Exception as e:
            logger.error(f"{LOG_CON_RABBIT}: {e}")
            raise

    def reconnectCamera(self):
        while True:
            try:
                logger.warning(f"{LOG_RECONNECT}: {self.camera_id}")
                # self.send_message({}, self.exchangerror)
                self.cap.release()
                self.cap = cv2.VideoCapture(self.camera_url)

                if not self.cap.isOpened():
                    raise Exception(f"No se pudo conectar a la camara: {self.camera_id}")
                self.loadParamsCamera()
                break
            except Exception as e:
                logger.error(f"{LOG_RECONNECT}: {e}")
                time.sleep(TIME_SLEEP_RECONNECT)

    def loadParamsCamera(self, umbral = UMBRAL_COUNT_PERCENT):
            self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            self.new_width  = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH) * self.size)
            self.new_height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT) * self.size)
            self.umbral     = int(umbral*self.new_width*self.new_height)

    def initCamera(self):
        self.cap = cv2.VideoCapture(self.camera_url, cv2.CAP_V4L2)
        if not self.cap.isOpened(): 
            self.reconnectCamera()
        self.loadParamsCamera()

    async def processVideo(self):
        try:
            ret, self.frame = self.cap.read()
            if not ret: return 401
            self.first_time = time.time()
            self.start_time             = time.time()
            self.data[NAME_EPOCH]       = int(self.start_time*EPOCH_FORMAT)
            self.data[NAME_CANDIDATE]   = True
            self.last_frame             = self.frame

            # saveImg(self.frame, self.data[NAME_EPOCH])

            # time_send_init = time.time()
            # if self.queuehost != "0.0.0.0":   send_message(self.channel, self.data, self.exchangename)
            # time_send_final = time.time()
            producer(self.frame, self.data)
            count = 0

            # logger.info(f'Iniciando grabacion de {self.camera_id } con first_time {self.first_time}, start_time {self.start_time} y con tiempo de envio {time_send_final-time_send_init}')
            while True:
                try:
                    # FPS de grabado
                    if (time.time() > self.start_time + self.interval_time):
                        self.data[NAME_EPOCH]      = int(time.time()*EPOCH_FORMAT)
                        self.data[NAME_CANDIDATE]  = False
                        self.data[NAME_STATUS]     = True

                        init_time_test = time.time()
                        ret, self.frame                 = self.cap.read()
                        final_time_test = time.time()
                        if final_time_test-init_time_test > UMBRAL_TIME_CAPTURE: logger.warning(f'{LOG_TIME_READ_CAP} es {final_time_test-init_time_test} mayor a {UMBRAL_TIME_CAPTURE} para la camara {self.camera_id}') 
                        if not ret: break
                        count += 1

                        # Indentify frame candidate
                        init_time_test = time.time()
                        if getFlagProccessed(self.frame, self.last_frame, self.umbral): 
                            self.data[NAME_CANDIDATE]   = True
                            self.data[NAME_STATUS]      = False
                            self.last_frame             = self.frame
                        final_time_test = time.time()
                        if final_time_test-init_time_test > UMBRAL_TIME_FILT_IMG: logger.warning(f'{LOG_TIME_FILTER} es {final_time_test-init_time_test} mayor a {UMBRAL_TIME_FILT_IMG} para la camara {self.camera_id}')
                        
                        producer(self.frame, self.data)
                        # # Save Image
                        # init_time_test = time.time()
                        # saveImg(self.frame, self.data[NAME_EPOCH])
                        # final_time_test = time.time()
                        # if final_time_test-init_time_test > UMBRAL_TIME_SAVE_IMG: logger.warning(f'{LOG_TIME_SAVE} es {final_time_test-init_time_test} mayor a {UMBRAL_TIME_SAVE_IMG} para la camara {self.camera_id}') 

                        # # Send Message
                        # init_time_test = time.time()
                        # if self.queuehost!="0.0.0.0": send_message(self.channel, self.data, self.exchangename)
                        # final_time_test = time.time()
                        # if final_time_test-init_time_test > UMBRAL_TIME_SEND_MSG: logger.warning(f'{LOG_TIME_SEND} es {final_time_test-init_time_test} mayor a {UMBRAL_TIME_SEND_MSG} para la camara {self.camera_id}')

                        self.start_time += self.interval_time 
                        lapse_time = time.time() - self.first_time
                        if lapse_time > 1:
                            if count != 5: logger.error(f'La cantidad de frames es {count} diferente a {self.fps_deseado}') 
                            count = 0
                            self.first_time += 1
                    else: self.cap.grab()
                except Exception as e:
                    logger.error(f"{LOG_PROCESS_IMG}: {e}")
                    return 402
            return 401
        except Exception as e:
            logger.critical(f"{LOG_MAIN_BUCLE}: {e}")
            return 403

def main():
    try:
        create_videos = generateVideos()
        create_videos.initCamera()        

        connection = aio_pika.connect_robust("amqp://root:user@10.23.63.63/")
        with connection:
            channel = connection.channel()

            workers = [asyncio.create_task(process_queue(channel)) for _ in range(3)]
            create_videos = generateVideos(camera_id=8, queuehost="127.0.0.1", channel=channel, exchangename="my_exchange")

            while True:
                try:
                    status      = create_videos.processVideo()
                    if status  != 401: 
                        break
                    logger.error(f"{LOG_READ_IMG}")
                    create_videos.reconnectCamera()

                except Exception as e:
                    logger.critical(f"{LOG_MAIN_BUCLE}: {e}")
                    break

            queue.join()

            for worker in workers:
                worker.cancel()
    except Exception as e:
        logger.critical(f"{LOG_MAIN_BUCLE}: {e}")

if __name__ == "__main__":
    main()

