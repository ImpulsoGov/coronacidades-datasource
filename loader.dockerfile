FROM tiangolo/uwsgi-nginx-flask:python3.7
# TODO use another image without flask optimizations

ENV OUTPUT_DIR="/output" \
    CONFIG_URL="https://raw.githubusercontent.com/ImpulsoGov/simulacovid/master/src/configs/config.yaml" \
    IS_PROD="False" \
    SLACK_WEBHOOK="" \
    INLOCO_CITIES_KEY="" \
    INLOCO_CITIES_ID="" \
    INLOCO_CITIES_ROUTE="" \
    INLOCO_STATES_KEY="" \
    INLOCO_STATES_ID="" \
    INLOCO_STATES_ROUTE="" \
    GOOGLE_TOKEN=""

ADD ./requirements.txt /app/

RUN pip install -r /app/requirements.txt

COPY ./src/loader /app/src/

RUN chmod +x /app/src/entrypoint.sh

ENTRYPOINT /app/src/entrypoint.sh

# HACK convert UWSGI container to one-time runner
RUN rm main.py prestart.sh uwsgi.ini

WORKDIR /app/src
