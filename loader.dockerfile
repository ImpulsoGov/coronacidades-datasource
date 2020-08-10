FROM tiangolo/uwsgi-nginx-flask:python3.7
# TODO use another image without flask optimizations

ENV OUTPUT_DIR="/output" \
    CONFIG_URL="https://raw.githubusercontent.com/ImpulsoGov/farolcovid/update-api-health-region/src/configs/config.yaml" \
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

# Remove R
RUN apt autoremove
RUN apt update

RUN apt install -y ca-certificates
RUN echo 'deb [trusted=yes] http://cloud.r-project.org/bin/linux/debian buster-cran40/' >> /etc/apt/sources.list
RUN apt -y update
RUN apt install -y -t buster-cran40 r-base

RUN R -e 'install.packages(c("RCurl", "EpiEstim", "tidyverse", "vroom", "TTR", "reticulate"), repo="http://cran.rstudio.com/")'

COPY ./src/loader /app/src/

RUN chmod +x /app/src/entrypoint.sh

ENTRYPOINT /app/src/entrypoint.sh

# HACK convert UWSGI container to one-time runner
RUN rm main.py prestart.sh uwsgi.ini

WORKDIR /app/src
