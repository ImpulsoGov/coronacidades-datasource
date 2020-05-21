FROM tiangolo/uwsgi-nginx-flask:python3.7

ENV OUTPUT_DIR=/output \
    ENDPOINTS_URL="https://raw.githubusercontent.com/ImpulsoGov/simulacovid-datasource/multiple-endpoints/src/loader/endpoints.yaml" \
    REFRESH_RATE_MINUTES=10

ADD ./requirements.txt /app/

RUN pip install -r /app/requirements.txt

COPY ./src/server /app
ENTRYPOINT python3 main.py
