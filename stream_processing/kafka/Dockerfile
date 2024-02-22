FROM python:3.9-slim

# Copy app handler code
WORKDIR /app
COPY create_table.py create_table.py
COPY insert_table.py insert_table.py
COPY run.sh .
COPY configs ./configs
COPY stream.parquet stream.parquet
COPY requirements.txt requirements.txt
COPY postgresql_client.py postgresql_client.py
COPY producer.sh producer.sh
RUN apt-get update && apt-get install -y curl
RUN pip install -r requirements.txt
RUN chmod o+rwx ./run.sh
RUN chmod o+rwx ./producer.sh
CMD [ "bash", "producer.sh" ]