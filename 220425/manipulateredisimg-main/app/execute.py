import os
import time
import logging
import cv2          # type: ignore
import numpy as np  # type: ignore
import pika         # type: ignore
import redis # type: ignore
import argparse  # type: ignore
 

from config import *
from config_logger import configurar_logger
from datetime import datetime

# logger = configurar_logger( name    = f"Execute", 
#                             log_dir = FILE_PATH_LOG,
#                             nivel   = logging.WARNING)

logger=logging.getLogger("Main")
r = redis.Redis(host=HOST_REDIS, port=PORT_REDIS, db=0, socket_timeout=5,
            decode_responses=False ) # Mantener datos como bytes

def getDate(epoch):
    dt = datetime.fromtimestamp(epoch)
    return dt.strftime(FILE_DATE), dt.strftime(FILE_HOUR)


def saveImg(frame, epoch, cam_id):

    inicio_tiempo = time.time()
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
        logger.info(f"Tiempo de escritura en disco: {time.time()-inicio_tiempo:.3f}")
        logger.info(f"Tiempo de escritura en disco: {time.time()-inicio_tiempo:.3f} - {output_path}/{epoch}.{FILE_FORMAT}")
    except IOError as e:
        logger.error(f"{LOG_SAVE_IMG}: {e}")

def saveImgObject(object, epoch, epoch_object, cam_id):

    inicio_tiempo = time.time()
    day, hour = getDate(epoch / EPOCH_FORMAT)
    output_path = f"/{FILE_OUTPUT}/{cam_id}/{day}/{hour}/{FILE_OBJECT}"
    
    os.makedirs(output_path, exist_ok=True)
    path_parts = output_path.split("/")
    current_path = "/"

    for part in path_parts:
        if part:
            current_path = os.path.join(current_path, part)
            os.chmod(current_path, 0o777)  

    try:
        result = cv2.imwrite(f"{output_path}/{epoch_object}.{FILE_FORMAT}", object)
        if not result:
            raise IOError("No se pudo guardar el objeto.")
        logger.info(f"Tiempo de escritura en disco object: {time.time()-inicio_tiempo:.3f}")
        logger.info(f"Tiempo de escritura en disco object: {time.time()-inicio_tiempo:.3f} - {output_path}/{epoch_object}.{FILE_FORMAT}")
    except IOError as e:
        logger.error(f"{LOG_SAVE_IMG}: {e}")

def getImg(id_camera, epoch): 
    # Obtener directamente el valor del hash (no hay JSON)
    inicio_tiempo = time.time()
    frame_set_key = f"{id_camera}{epoch}"
    objects_set_key = f"{id_camera}{epoch}:object"

    logger.info(f"objects_set_key: {objects_set_key}")
    
    image_bytes = r.get(frame_set_key)
    
    if not image_bytes:
        logger.error(f"Frame {id_camera} no existe en Redis")
        return
        
    logger.info(f"Tiempo obtención Redis: {time.time() - inicio_tiempo:.3f}s")

    try:
        #Decodificar a numpy array
        np_arr = np.frombuffer(image_bytes, dtype=np.uint8)
        logger.info(f"Tiempo buffer to array: {time.time() - inicio_tiempo:.3f}")
        img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        logger.info(f"Tiempo array to img: {time.time() - inicio_tiempo:.3f}")
        
        if img is None:
            logger.error("Formato de imagen inválido")
            return
        
        saveImg(frame=img,epoch=epoch,cam_id=id_camera)

        r.delete(frame_set_key)
        logger.info(f"Se elimino de redis: Camara - {id_camera} / epoch: {epoch}")

        # Obtener todas las claves de objetos asociados desde el set
        object_keys = r.smembers(objects_set_key)
        logger.info(f"Los objectos son: {object_keys}")

        for object_key in object_keys:
            texto = object_key.decode("utf-8")  # Convierte de bytes a string
            object_id = texto.strip("()").split(":")[2]
            logger.info(f"object_id: {object_id}")  
            
            image_bytes_object = r.get(object_id)
            if image_bytes_object:
                # Decodificar a numpy array object
                np_arr_object = np.frombuffer(image_bytes_object, dtype=np.uint8)
                logger.info(f"Tiempo buffer to array: {time.time() - inicio_tiempo:.3f}")
                img_object = cv2.imdecode(np_arr_object, cv2.IMREAD_COLOR)
                logger.info(f"Tiempo array to img object: {time.time() - inicio_tiempo:.3f}")
                
                if img_object is None:
                    logger.error("Formato de imagen de objeto inválido")
                    return
                epoch_object = object_id[1:]
                saveImgObject(object=img_object,epoch=epoch, epoch_object=epoch_object,cam_id=id_camera)  

        if object_keys:
            r.delete(*object_keys)
 
    except Exception as e:
        logger.error(f"Error decodificación: {str(e)}")            
                   
        
# def main():
#     inicio_tiempo = time.time()

#     getImg(NAME_CAMERA, value)

    # parser = argparse.ArgumentParser(description="Recibir variables en un contenedor")
    # parser.add_argument("--camera", type=str, help="Nombre de la camara")
    # parser.add_argument("--epoch", type=int, help="Fecha en Unix Timestamp")

    # args = parser.parse_args()

    # getImg(args.camera, args.epoch)


    # Obtener todas las claves dentro del hash de la cámara
    #all_epochs = r.hkeys("*")
    
#     all_epochs=r.hgetall("NAME_CAMERA")
#     print("epochs:", all_epochs)

#     # Filtrar las claves dentro del rango especificado
#     filtered_epochs = [epoch for epoch in all_epochs if int(epoch) <= inicio_tiempo-300]
#     print("epochs filtrados:", filtered_epochs)

#     # Obtener los valores correspondientes a los epochs filtrados
#     data_in_range = {epoch: r.hget(NAME_CAMERA, NAME_EPOCH) for epoch in filtered_epochs}
        
#     # Mostrar resultados
#     for epoch, value in data_in_range.items():
#         print(f"Epoch: {epoch}, Value: {value}")
#         #getImg(NAME_CAMERA, value)


# hash_keys = [key for key in r.keys("*") if r.type(key) == NAME_CAMERA]

# # Leer todos los valores de cada hash y sumar los valores numéricos
# hashes_data = {}

# for hash_name in hash_keys:
#     data = r.hgetall(NAME_CAMERA)  # Obtener los datos del hash
#     suma_total = sum(int(value) for value in data.values() if value.isdigit())  # Sumar solo valores numéricos
#     hashes_data[hash_name] = {"datos": data, "suma": suma_total}  # Guardar datos y suma

# # Mostrar los hashes y sus valores sumados
# for hash_name, info in hashes_data.items():
#     print(f"\n🔹 Hash: {hash_name}")
#     for field, value in info["datos"].items():
#         print(f"  {field}: {value}")



# if __name__ == "__main__":
#     logger.info("Inicio de programa")
#     main()



