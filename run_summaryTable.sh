docker stop summarytable
docker rm summarytable
docker run --name summarytable -itd -v /etc/localtime:/etc/localtime:ro -v /app_1/wma/MT_SUMMARY/ST:/ST --device /dev/fuse --cap-add SYS_ADMIN summarytable
docker exec -itd summarytable python run.py
