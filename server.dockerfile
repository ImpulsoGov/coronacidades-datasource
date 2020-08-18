FROM tiangolo/uwsgi-nginx-flask:python3.7

ENV OUTPUT_DIR=/output \
    REFRESH_RATE_MINUTES=10

ADD ./requirements.txt /app/

# Remove R
RUN apt autoremove
RUN apt update

RUN apt install -y ca-certificates
RUN echo 'deb [trusted=yes] http://cloud.r-project.org/bin/linux/debian buster-cran40/' >> /etc/apt/sources.list
RUN apt -y update
RUN apt install -y -t buster-cran40 r-base

RUN pip install -r /app/requirements.txt

COPY ./src/server /app
ENTRYPOINT python3 main.py
