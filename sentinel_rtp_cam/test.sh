#!/bin/bash

docker run --rm -it \                                                                                                                                                                                   <aws:impact-engineers> <region:us-west-2>
  -p 8554:8554/tcp \
  -p 8000:8000/udp \
  -p 8001:8001/udp \
  bluenviron/mediamtx:latest

ffmpeg -re -f lavfi -i testsrc=size=1920x1080:rate=25 \
  -c:v libx264 -preset veryfast -tune zerolatency -pix_fmt yuv420p \
  -g 50 -keyint_min 50 -sc_threshold 0 \
  -x264-params repeat-headers=1 \
  -f rtsp rtsp://127.0.0.1:8554/cam
