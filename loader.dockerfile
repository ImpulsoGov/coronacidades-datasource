FROM tiangolo/uwsgi-nginx-flask:python3.7

ENV OUTPUT_DIR=/output \
    RAW_NAME=raw \
    CONFIG_URL="https://raw.githubusercontent.com/ImpulsoGov/simulacovid/master/src/configs/config.yaml" \
    REFRESH_RATE_MINUTES=1

ADD ./requirements.txt /app/

RUN pip install -r /app/requirements.txt

COPY ./src/loader /app

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT /app/entrypoint.sh
