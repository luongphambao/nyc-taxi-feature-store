This repo is used for 3 lessons, so it will be a bit complicated, but don't worry about it, said Prof. Andrew Ng.

## How-to Guide

### Up and running services
Start Prometheus, Grafana (to see metrics), and Jaeger Tracing (to see traces) as follows

```shell
docker compose -f prom-graf-docker-compose.yaml up -d
```

Start ELK stack to see container logs by the following command:
```shell
cd elk
docker compose -f elk-docker-compose.yml -f extensions/filebeat/filebeat-compose.yml up -d
```

### Access services
- Grafana: http://localhost:3000 with `username/password` is `admin/admin`
- Kibana: http://localhost:5601 with `username/password` is `elastic/changeme`
- Jaeger: http://localhost:16686
