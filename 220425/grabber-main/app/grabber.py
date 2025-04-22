import os
import time
import logging
import asyncio
import json
import cv2          # type: ignore
import numpy as np  # type: ignore
import pika         # type: ignore
import queue
import redis        # type: ignore

from datetime import datetime
from config_logger import configurar_logger
from publisher import ExampleSyncPublisher
from config import *
from threading import Thread

logger  = configurar_logger( name    = f"{FILE_NAME_LOG}_{os.environ.get(CAMARAID)}", 
                            log_dir = FILE_PATH_LOG,
                            nivel   = logging.WARNING)
queue   = queue.Queue(maxsize=QUEUE_MAXSIZE)  
r       = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

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

    try:
        result = cv2.imwrite(f"{output_path}/{epoch}.{FILE_FORMAT}", frame)
        if not result:
            raise IOError("No se pudo guardar la imagen.")
    except IOError as e:
        logger.error(f"{LOG_SAVE_IMG}: {e}")

def process_queue_sync():
    """Procesa la cola de frames y los vacía uno por uno"""
    publisher = ExampleSyncPublisher(AMQP_URL)
    while True:
        frame, data = queue.get()
        try:
            init_time_test = time.time()
            _, buffer = cv2.imencode('.jpeg', frame)
            jpg_bytes = buffer.tobytes()
            r.set(f"{data[NAME_CAMERA]}{data[NAME_EPOCH]}", jpg_bytes, ex=4)
            # logger.warning(f'hset es {time.time()-init_time_test} para la camara {data[NAME_CAMERA]}')

            init_time_test = time.time()
            publisher.publish(data)
            publisher.publish_delayed(data, delay_ms=REDIS_TIME)

            final_time_test = time.time()
            if final_time_test-init_time_test > UMBRAL_TIME_SEND_MSG: logger.warning(f'{LOG_TIME_SEND} es {final_time_test-init_time_test} mayor a {UMBRAL_TIME_SEND_MSG} para la camara {data[NAME_CAMERA]}')

        except Exception as e:
            logger.error(f"Error procesando frame en la cola: {e}")
        finally:
            queue.task_done()

def producer(frame, data):
    if queue.full():  
        try:
            logger.warning(f'el tamano de la cola es: {queue.qsize()}')
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

        self.exchangename   = os.environ.get(EXCHANGENAME, None)
        self.exchangerror   = os.environ.get(EXCHANGERROR, None)
        self.zonerestricted = os.environ.get(ZONERESTRICTED, None)

        self._connection    = None
        self._channel       = None
        self.last_frame     = None   
        self.frame          = None
        self._publisher     = None
        
        thread = Thread(target=process_queue_sync, daemon=True)
        thread.start()

        self.interval_time  = 1 / self.fps_deseado
        self.fps_processed  = [round((i + 1) * self.fps_deseado / PROCESSED_FRAME) for i in range(PROCESSED_FRAME)]
        self.data           = {NAME_CAMERA : self.camera_id, NAME_FUNC : [], NAME_STATUS : False, NAME_ZONERESTRICTED : self.zonerestricted}

        logger.info(f"{LOG_CONFIG_CAM} {self.camera_id}, URL: {self.camera_url}, FPS: {self.fps_deseado}, SIZE: {self.size}")

    def getFlagProccessed(self,
                        umbral  = UMBRAL_COUNT_PIXELS, 
                        umbral2 = UMBRAL_REST_PIXEL):
        try:
            if self.last_frame is None:
                return True
            _, thresh = cv2.threshold(
                cv2.absdiff(cv2.cvtColor(self.frame, cv2.COLOR_BGR2GRAY), cv2.cvtColor(self.last_frame, cv2.COLOR_BGR2GRAY)),
                umbral2, 255, cv2.THRESH_BINARY)
            return True if np.count_nonzero(thresh) > umbral else False
        except Exception as e:
            logger.error(f"{LOG_FUNC_FLAG}: {e}")
            return False

    def loadParamsCamera(self, umbral = UMBRAL_COUNT_PERCENT):
            self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            self.new_width  = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH) * self.size)
            self.new_height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT) * self.size)
            self.umbral     = int(umbral*self.new_width*self.new_height)

    def initCamera(self):
        self.cap = cv2.VideoCapture(self.camera_url, cv2.CAP_FFMPEG)
        if not self.cap.isOpened(): 
            self.reconnectCamera()
        self.loadParamsCamera()

    def reconnectCamera(self):
        while True:
            try:
                logger.warning(f"{LOG_RECONNECT}: {self.camera_id}")
                self.cap.release()
                self.cap = cv2.VideoCapture(self.camera_url, cv2.CAP_FFMPEG)

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
        self.cap = cv2.VideoCapture(self.camera_url, cv2.CAP_FFMPEG)
        if not self.cap.isOpened(): 
            self.reconnectCamera()
        self.loadParamsCamera()

    def processVideo(self):
        try:
            ret, self.frame = self.cap.read()
            if not ret: return 401

            # self.frame = cv2.resize(self.frame, HD)
            self.first_time             = time.time()
            self.start_time             = time.time()
            self.data[NAME_EPOCH]       = int(self.start_time*EPOCH_FORMAT)
            self.data[NAME_CANDIDATE]   = True
            self.last_frame             = self.frame
            count = 0

            producer(self.frame.copy(), self.data.copy())

            while True:
                try:
                    # FPS de grabado
                    if (time.time() > self.start_time + self.interval_time):
                        self.data[NAME_EPOCH]      = int(time.time()*EPOCH_FORMAT)
                        self.data[NAME_CANDIDATE]  = False
                        self.data[NAME_STATUS]     = True

                        init_time_test = time.time()
                        ret, self.frame = self.cap.read()
                        final_time_test = time.time()
                        if final_time_test-init_time_test > UMBRAL_TIME_CAPTURE: logger.warning(f'{LOG_TIME_READ_CAP} es {final_time_test-init_time_test} mayor a {UMBRAL_TIME_CAPTURE} para la camara {self.camera_id}') 
                        if not ret: break
                        # self.frame = cv2.resize(self.frame, HD)
                        count += 1

                        # Indentify frame candidate
                        init_time_test = time.time()
                        if getFlagProccessed(self.frame, self.last_frame, self.umbral): 
                            self.data[NAME_CANDIDATE]   = True
                            self.data[NAME_STATUS]      = False
                            self.last_frame             = self.frame
                        final_time_test = time.time()
                        if final_time_test-init_time_test > UMBRAL_TIME_FILT_IMG: logger.warning(f'{LOG_TIME_FILTER} es {final_time_test-init_time_test} mayor a {UMBRAL_TIME_FILT_IMG} para la camara {self.camera_id}')

                        producer(self.frame.copy(), self.data.copy())

                        lapse_time = time.time() - self.first_time
                        if lapse_time > 10:
                            if count != 10*self.fps_deseado: logger.warning(f'La cantidad de frames es {count} diferente a {10*self.fps_deseado} en {lapse_time}') 
                            count = 0
                            self.first_time += 10

                        self.start_time += ((time.time() - self.start_time) // self.interval_time) * self.interval_time
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

    except Exception as e:
        logger.critical(f"{LOG_MAIN_BUCLE}: {e}")

if __name__ == "__main__":
    main()

