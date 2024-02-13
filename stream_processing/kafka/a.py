docker build -t nyc_producer:latest .
docker image tag nyc_producer:latest luongphambao/nyc_producer:latest
docker push
#run docker 
docker run -d --network host nyc_producer:latest