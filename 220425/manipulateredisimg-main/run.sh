docker stop redisImage
docker rm redisImage

docker run  \
--restart=always \
-d \
-v ./app:/app \
-v ./log:/log \
-v /mnt/sda/develops/cameradata:/output \
--network host \
--name redisImage  \
manipulate_redisimg:202503
