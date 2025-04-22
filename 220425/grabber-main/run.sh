

docker stop cameraManager
docker rm cameraManager

docker run  \
--restart=always \
-d  \
-v ./app:/app \
-v ./log:/log \
-v /var/run/docker.sock:/var/run/docker.sock \
-p 5672:5672 -p 15672:15672 \
--network host \
--name cameraManager  \
camera_manager:202502
