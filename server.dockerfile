FROM tiangolo/uwsgi-nginx-flask:python3.7

ENV OUTPUT_DIR=/output \
    REFRESH_RATE_MINUTES=10

ADD ./requirements.txt /app/

RUN pip install -r /app/requirements.txt

COPY ./src/server /app
ENTRYPOINT python3 main.py
