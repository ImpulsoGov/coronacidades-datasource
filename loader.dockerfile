FROM tiangolo/uwsgi-nginx-flask:python3.7

ENV OUTPUT_DIR=/output

ADD ./requirements.txt /app/

RUN pip install -r /app/requirements.txt

COPY ./src/loader /app

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT /app/entrypoint.sh
