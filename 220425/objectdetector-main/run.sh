docker stop objectDetector
docker rm objectDetector

# -v /SSD500/mvp/project2/app/output/images/25/2025-11-02/21/frames:/app/frames \

docker run  \
-d  \
-e TEST_MODE=false \
-v ./app:/app \
-v ./log:/log \
--network host \
--name objectDetector  \
--gpus all  \
objectDetector:202504
