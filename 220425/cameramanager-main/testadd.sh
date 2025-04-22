cameraid=35
imagedocker=grabber_img:202504
url="rtsp://admin:Camarav1d3o\$@192.168.219.113:554/IREX/media.smp"

fps=5
size=1

target=/mnt/sda/develops/cameradata/
funcionality=vehicles
queuehost=10.23.63.54
queueport='5672'
queueuser=root
queuepass=winempresas
exchangename=frames
exchangerror=errorContainer
zonerestricted=""

data='{'\
'"cameraid":"'$cameraid'",'\
'"imagedocker":"'$imagedocker'",'\
'"url":"'$url'",'\
'"fps":"'$fps'",'\
'"size":"'$size'",'\
'"target":"'$target'",'\
'"funcionality":"'$funcionality'",'\
'"queuehost":"'$queuehost'",'\
'"queueport":"'$queueport'",'\
'"queueuser":"'$queueuser'",'\
'"queuepass":"'$queuepass'",'\
'"exchangename":"'$exchangename'",'\
'"zonerestricted":"'$zonerestricted'"'\
'}'

echo $data
echo $data | curl -H "Content-Type: application/json" \
-X POST http://10.23.63.82:5000/add_camera \
-d $data 
