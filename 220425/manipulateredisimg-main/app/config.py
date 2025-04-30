import logging


# parameters environment
URL         = 'URL'
CAMARAID    = 'CAMARAID'
FPS         = 'FPS'
SIZE        = 'SIZE'
QUEUEHOST   = 'QUEUEHOST'
QUEUEPORT   = 'QUEUEPORT'
QUEUEUSER   = 'QUEUEUSER'
QUEUEPASS   = 'QUEUEPASS'
EXCHANGENAME= 'EXCHANGENAME'
EXCHANGERROR= 'EXCHANGERROR'

# parameters client rabbitMQ
RABBITMQ_HOST   = '10.23.63.56' 
RABBITMQ_PORT   = '5672' 
RABBITMQ_USER   = 'root'
RABBITMQ_PASS   = 'winempresas'
RABBITMQ_QUEUE  = 'frame_register_queue'
EXCHANGE_TYPE   = 'fanout'
EXCHANGE_ROUTING= ''  # En fanout, no se usa routing_key
EXCHANGE_DURABLE= True
EXCHANGE_DELMODE= 2
AMQP_URL        = 'amqp://root:winempresas@10.23.63.56:5672/%2F'


HOST_REDIS  =  '10.23.63.56' 
PORT_REDIS  = 6379



# parameters data
NAME_EPOCH      = 'epoch_frame'
NAME_CAMERA     = 'camera_id'
NAME_FUNC       = 'funcionality'
NAME_STATUS     = 'status_frame'
NAME_CANDIDATE  = 'candidate_frame'

# parameters log 
FILE_NAME_LOG   = 'grabber'
FILE_LOG_PUBLI  = 'publisher'
FILE_PATH_LOG   = './log'
LEVEL_LOG       = logging.DEBUG

# parameters umbral frames
UMBRAL_COUNT_PERCENT= 0.1
UMBRAL_COUNT_PIXELS = 20000
UMBRAL_REST_PIXEL   = 30

# parameters umbral times
UMBRAL_TIME_CAPTURE = 0.06
UMBRAL_TIME_SAVE_IMG= 0.05
UMBRAL_TIME_FILT_IMG= 0.02
UMBRAL_TIME_SEND_MSG= 0.05

# parameters save image
FILE_DATE   = '%Y-%m-%d'
FILE_HOUR   = '%H'
FILE_OUTPUT = 'output'
FILE_FRAME  = 'frames'
FILE_OBJECT  = 'object'
FILE_FORMAT = 'jpeg'
EPOCH_FORMAT= 1000

# parameters conect camera
TIME_SLEEP_RECONNECT = 5

# parameters processed frames
PROCESSED_FRAME = 5

# resolutions
FHD = (1980, 1080)
HD = (1280,720)

# parameters logger error
LOG_CONFIG_CAM    = '300 - Configuracion de camara'
LOG_CONFIG_RABBIT = '330 - Configuracion de rabbitMQ'
LOG_RECIB_MSG     = '331 - Mensaje recibido'
LOG_SEND_MSG      = '331 - Mensaje publicado'

LOG_PROCESS_IMG   = '410 - Procesamiento de imagen'
LOG_TIME_FILTER   = '411 - Tiempo de decision de frame candidato'
LOG_SAVE_IMG      = '412 - Escritura de imagen'
LOG_READ_IMG      = '413 - Lectura de imagen'
LOG_TIME_SAVE     = '414 - Tiempo de escritura'
LOG_TIME_READ     = '415 - Tiempo de lectura'

LOG_CON_RTSP      = '403 - Conexion a la camara'
LOG_FRAME         = '404 - Captura del fotograma'
LOG_RECONNECT     = '406 - Reconexion de camara'
LOG_PARAMETERS    = '407 - Al inicializar parametros'
LOG_TIME_READ_CAP = '408 - Tiempo captura del frame'

LOG_CON_RABBIT    = '430 - Conexion a RabbitMQ'
LOG_PUBLISHER     = '431 - Publicacion del mensaje'
LOG_CONSUMER      = '431 - Obtencion del mensaje'
LOG_TIME_SEND     = '432 - Tiempo de envio de frame'

LOG_MAIN_BUCLE    = '450 - Inesperado en bucle principal'
LOG_FUNC_FLAG     = '451 - Discriminacion del frame candidato'

