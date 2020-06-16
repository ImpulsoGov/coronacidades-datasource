FROM tiangolo/uwsgi-nginx-flask:python3.7

ENV OUTPUT_DIR=/output \
    CONFIG_URL="https://raw.githubusercontent.com/ImpulsoGov/simulacovid/stable/src/configs/config.yaml" \
    IS_PROD=False

ADD ./requirements.txt /app/

RUN pip install -r /app/requirements.txt

COPY ./src/loader /app

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT /app/entrypoint.sh
