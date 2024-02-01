## How-to Guide

Start the docker compose

```shell
docker compose -f docker-compose.yaml up -d
```

Try changing `expectations` in `strawberry_suite.json` located at the path `include/great_expectations/expectations/strawberry_suite.json` to see the magic behind `GreatExpectationsOperator`.

## DAGs

```shell
.
├── demo.py: A hello world dag
├── gx.py: DAG using GreatExpectationsOperator
├── lightgbm
│   ├── build.sh
│   ├── Dockerfile
│   ├── docker.py: DAG using DockerOperator
│   ├── requirements.txt
│   └── train.py
├── python.py: DAG using Python code
└── sklearn.py: DAG using Python code running in a virtualenv

```