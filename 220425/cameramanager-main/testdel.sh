cameraid=9
imagedocker=grabber_img:202502
url=rtsp://admin:Camarav1d3o\$@192.168.217.101:554/IREX/media.smp
fps=5
size=1
target=/mnt/sda/develops/cameradata/
target=/mnt/sda/develops/cameradata/
queuehost=0.0.0.0
queueout=cameraout

data='{'\
'"cameraid":"'$cameraid'",'\
'"imagedocker":"'$imagedocker'",'\
'"url":"'$url'",'\
'"fps":"'$fps'",'\
'"size":"'$size'",'\
'"target":"'$target'",'\
'"queuehost":"'$queuehost'",'\
'"queueout":"'$queueout'"'\
'}'

echo $data
echo $data | curl -H "Content-Type: application/json" \
-X POST http://10.23.63.52:5000/remove_camera \
-d $data 