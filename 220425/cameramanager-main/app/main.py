import docker  # type: ignore
import logging
import os
from flask import Flask, request, jsonify # type: ignore
from config_logger import configurar_logger

app = Flask(__name__)
docker_client = docker.from_env(timeout=300)
BASE_DIR = os.path.abspath(os.getcwd()) 

logger = configurar_logger(
    name="camera_manager", 
    log_dir="/log",
    nivel=logging.DEBUG
    )

# Agregar una cámara (crear un contenedor para ella)
@app.route('/add_camera', methods=['POST'])
def add_camera():
    data = request.json
    # logger.info(f'200-{data}')
    cameraid    = data.get("cameraid")
    imagedocker = data.get("imagedocker")
    url         = data.get("url")
    fps         = data.get("fps")
    size        = data.get("size")
    target      = data.get("target")
    funcionality= data.get("funcionality")
    redishost   = data.get("redishost")
    redisport   = data.get("redisport")
    redistime   = data.get("redistime")
    amqp        = data.get("amqp")
    exchangename= data.get("exchangename")
    exchangerror= data.get("exchangerror")
    core_selected= data.get("core_selected","0")
    zonerestricted= data.get("zonerestricted", None)

    logger.info(f'200 - starting camera {cameraid} with url: {url}')
    if not cameraid or not url:
        e = "ID y URL son obligatorios"
        logger.info(f'401  - {e}')

        return jsonify({e}), 400

    # Crear contenedor dinámico para la cámara
    container_name = f"camera_{cameraid}"

    docker_client.containers.run(
        imagedocker,
        detach=True,
        name=container_name,
        environment={
            "CAMARA_ID"     : cameraid, 
            "URL"           : url,
            "FPS"           : fps,
            "SIZE"          : size,
            "FUNCIONALITY"  : funcionality,
            "REDIS_HOST"      : redishost,
            "REDIS_PORT"      : redisport,
            'REDIS_TIME_DELAY': redistime, 
            "AMQP_URL"        : amqp,
            "EXCHANGE_NAME"   : exchangename,
            "EXCHANGE_ERROR"  : exchangerror,
            "ZONE_RESTRICTED" : zonerestricted
        },
        network="host",
        volumes={
            f'{target}/app': {"bind": "/app", "mode": "rw"},
            f"{target}/log": {"bind": "/app/log", "mode": "rw"}
        },
        restart_policy={"Name": "unless-stopped"},
        cpuset_cpus=core_selected
    )
    return jsonify({"message": f"Cámara {cameraid} agregada y contenedor creado"}), 200

# Eliminar una cámara (detener y eliminar su contenedor)
@app.route('/remove_camera', methods=['POST'])
def remove_camera():
    # print('hola')
    data = request.json
    cameraid    = data.get("cameraid")

    if not cameraid:
        return jsonify({"error": "ID es obligatorio"}), 400

    # Eliminar contenedor
    container_name = f"camera_{cameraid}"
    try:
        container = docker_client.containers.get(container_name)
        container.stop()
        container.remove()
        return jsonify({"message": f"El contenedor de la cámara {cameraid} fue contenedor detenido y eliminado"}), 200
    except docker.errors.NotFound:
        return jsonify({"error": f"Contenedor {cameraid}, no encontrado"}), 404

# Nueva ruta para actualizar una cámara (detener y volver a crear el contenedor)
@app.route('/update_camera', methods=['POST'])
def update_camera():
    data = request.json
    camera_id = data.get("CAMARA_ID")

    if not camera_id:
        return jsonify({"error": "ID es obligatorio"}), 400

    # Llamar a add_camera() para crear la nueva cámara con los nuevos parámetros
    return add_camera()

# Listar cámaras activas
@app.route('/get_cameras', methods=['GET'])
def get_cameras():
    containers = docker_client.containers.list(all=True, filters={"name": "camera_"})
    return jsonify(containers), 200

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)
