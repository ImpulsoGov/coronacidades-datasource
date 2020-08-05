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

# Install R new -- doesn't work, still 3.5.2
RUN apt-get install -y software-properties-common 
RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/'
RUN gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
RUN gpg -a --export E298A3A825C0D65DFD57CBB651716619E084DAB9 | apt-key add -
RUN apt install -y r-base r-base-core r-recommended r-base-dev

# Another try -- broken packages
# RUN echo "deb http://www.stats.bris.ac.uk/R/bin/linux/ubuntu trusty-cran35/" >> /etc/apt/sources.list
# RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
# RUN apt-get -y update
# RUN apt-get -y upgrade
# RUN apt-get -y dist-upgrade
# RUN apt install -y r-base r-base-core r-recommended r-base-dev

# RUN apt-get install r-base=4.0.2
# RUN R -e 'install.packages(c("RCurl"), repo="http://cran.rstudio.com/")'
# RUN R -e 'install.packages(c("EpiEstim"), repo="https://cloud.r-project.org/")'

COPY ./src/loader /app/src/

RUN chmod +x /app/src/entrypoint.sh

ENTRYPOINT /app/src/entrypoint.sh

# HACK convert UWSGI container to one-time runner
RUN rm main.py prestart.sh uwsgi.ini

WORKDIR /app/src
