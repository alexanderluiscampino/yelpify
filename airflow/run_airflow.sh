docker run --rm \
    --name airflow_local -d -p 8080:8080 \
    -v $(pwd)/requirements.txt:/requirements.txt \
    -v $(pwd)/dags:/usr/local/airflow/dags \
    -v $(pwd)/plugins/:/usr/local/airflow/plugins \
puckel/docker-airflow webserver 