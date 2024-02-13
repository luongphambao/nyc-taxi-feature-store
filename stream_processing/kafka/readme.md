docker build -t nyc_producer:latest .
docker image tag nyc_producer:latest luongphambao/nyc_producer:latest
docker push luongphambao/nyc_producer:latest