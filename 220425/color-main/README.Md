# objectCandidate

Contenedor de procesamiento. Deteccion de atributo color de los objetos candidatos.

---

## Drivers

- Docker https://docs.docker.com/engine/install/ubuntu/
- Cuda Drivers https://docs.nvidia.com/cuda/cuda-installation-guide-linux/#driver-installation
- Cuda toolkit para docker https://docs.nvidia.com/ai-enterprise/deployment/vmware/latest/docker.html

## Imagen docker

### Imagen base 

python:3.11-slim

Imagen python slim

### Librerias complementarias. Requirements.txt

- Conexion a rabbitmq. pika==1.3.2
- Estructura de logs. pytz==2024.2
- Conexion a Redis. redis==5.2.1
- Procesamiento de imagenes. opencv-python==4.10.0.84

## Librerias

- cv2 (OpenCV) – Procesamiento de imágenes.
- numpy – Computación numérica.
- redis – Interacción con bases de datos Redis.
- pika (no aparece explícitamente, pero publisher y consumer sugieren uso de RabbitMQ).

## Dependencias

Rabbitmq. Consumidor y publicador

Redis imagenes. Acceder a las imagenes de procesamiento

## Logica de procesamiento



## Secuencia de ejecucion

### build.sh Construccion de la imagen

```bash
TAGNAME=$(basename "$PWD"):$(date +'%Y%m')      # Nombre de Imagen:Tag
docker rmi $TAGNAME                             # Remover imagen anterior
docker build -t $TAGNAME .                      # Construir imagen
```

### run.sh Levantar el contenedor

```bash
docker stop color      # Stop el contenedor si esta levantado
docker rm color        # Remover el contenedor si esta stopeado

docker run  \
-d  \                           # Ejecucion por detras
-e TEST_MODE=false \            # Variable de entorno
-v ./app:/app \                 # Volumen de app
-v ./log:/log \                 # Volumen de logs
--network host \                # Red interna de contendores
--name color  \        # Nombre del contenedor
color:202504           # Imagen:Tag
```

## Logs

### consumer.log 

Logs de consumidor de mensajes de RabbitMQ.

Conexion correcta:

```bash
2025-04-16 11:10:09 - INFO - consumer:47 - Connecting to amqp://root:winempresas@10.23.63.56:5672/%2F
2025-04-16 11:10:09 - INFO - consumer:63 - Connection opened
2025-04-16 11:10:09 - INFO - consumer:83 - Creating a new channel
2025-04-16 11:10:09 - INFO - consumer:87 - Channel opened
2025-04-16 11:10:09 - INFO - consumer:93 - Adding channel close callback
2025-04-16 11:10:09 - INFO - consumer:101 - Declaring exchange: frames
2025-04-16 11:10:09 - INFO - consumer:110 - Exchange declared: frames
2025-04-16 11:10:09 - INFO - consumer:114 - Declaring queue frame_register
2025-04-16 11:10:09 - INFO - consumer:120 - Binding frames to frame_register with 
2025-04-16 11:10:09 - INFO - consumer:130 - Queue bound: frame_register
2025-04-16 11:10:09 - INFO - consumer:138 - QOS set to: 1
2025-04-16 11:10:09 - INFO - consumer:142 - Issuing consumer related RPC commands
2025-04-16 11:10:09 - INFO - consumer:150 - Adding consumer cancellation callback
```

### color.log

Logs de procesamiento de imagenes

Procesamiento correcto

```bash
Datos recibidos: {'camera_id': '35', 'funcionality': [], 'status_frame': True, 'zone_restricted': {'accident': {'coords': [[0.0, 0.1827], [0.6268, 0.0726], [0.7139, 1.0], [0.0, 1.0]], 'recurrence_time': 60000}}, 'epoch_frame': 1744411214227, 'candidate_frame': True, 'init_time_frame': 1744411214555, 'n_objects': 7, 'object_dict': {'3': [{'coords': [369, 141, 671, 315], 'accuracy': 90, 'epoch_object': 1744411214602, 'candidate': True, 'status_object': False, 'atributtes_list': [1, 2], 'object_id': '35174441121422730', 'id_tracking': '35174441121142430', 'best_accuracy': 90}, {'coords': [518, 180, 872, 411], 'accuracy': 89, 'epoch_object': 1744411214602, 'candidate': True, 'status_object': False, 'atributtes_list': [1, 2], 'object_id': '35174441121422731', 'id_tracking': '35174441121401633', 'best_accuracy': 89}, {'coords': [913, 98, 1044, 186], 'accuracy': 80, 'epoch_object': 1744411214603, 'candidate': False, 'status_object': False, 'atributtes_list': [1, 2], 'object_id': '35174441121422732', 'id_tracking': '35174441121241734', 'best_accuracy': 84}, {'coords': [0, 220, 292, 377], 'accuracy': 65, 'epoch_object': 1744411214603, 'candidate': False, 'status_object': False, 'atributtes_list': [1, 2], 'object_id': '35174441121422733', 'id_tracking': '35174441121202432', 'best_accuracy': 90}, {'coords': [1221, 73, 1279, 123], 'accuracy': 62, 'epoch_object': 1744411214603, 'candidate': True, 'status_object': False, 'atributtes_list': [1, 2], 'object_id': '35174441121422734', 'id_tracking': '35174441121362636', 'best_accuracy': 62}, {'coords': [1, 345, 379, 694], 'accuracy': 52, 'epoch_object': 1744411214604, 'candidate': True, 'status_object': False, 'atributtes_list': [1, 2], 'object_id': '35174441121422735', 'id_tracking': '35174441121401631', 'best_accuracy': 52}, {'coords': [1140, 70, 1209, 104], 'accuracy': 50, 'epoch_object': 1744411214605, 'candidate': False, 'status_object': True, 'atributtes_list': [1, 2], 'object_id': '35174441121422736', 'id_tracking': '35174441121422736'}]}, 'final_time_frame': 1744411214605, 'shape': [720, 1280, 3]}
2025-04-11 17:40:14 - INFO - main:82 - Tiempo obtención Redis: 0.001s
2025-04-11 17:40:14 - INFO - main:86 - Tiempo buffer to array: 0.001s
2025-04-11 17:40:14 - INFO - main:88 - Tiempo array to img: 0.002s
2025-04-11 17:40:14 - INFO - main:82 - Tiempo obtención Redis: 0.004s
2025-04-11 17:40:14 - INFO - main:86 - Tiempo buffer to array: 0.004s
```
### publisher.log

Logs de publicador de mensajes de RabbitMQ

Conexion correcta:

```bash
2025-04-16 11:10:17 - INFO - publisher:238 - Publisher thread started.
2025-04-16 11:10:17 - INFO - publisher:59 - Connection opened
2025-04-16 11:10:17 - INFO - publisher:85 - Creating a new channel
2025-04-16 11:10:17 - INFO - publisher:92 - Channel opened
2025-04-16 11:10:17 - INFO - publisher:101 - Adding channel close callback
2025-04-16 11:10:17 - INFO - publisher:117 - Declaring exchange objs_detected
2025-04-16 11:10:17 - INFO - publisher:130 - Exchange declared: objs_detected. Publisher is ready.
2025-04-16 11:10:17 - INFO - publisher:137 - Enabling delivery confirmations
```