FROM tiangolo/uwsgi-nginx-flask:python3.7
# TODO use another image without flask optimizations

ENV OUTPUT_DIR="/output" \
    CONFIG_URL="https://raw.githubusercontent.com/ImpulsoGov/farolcovid/capacity-update/src/configs/config.yaml" \
    IS_PROD="False" \
    SLACK_WEBHOOK="" \
    INLOCO_CITIES_KEY="" \
    INLOCO_CITIES_ID="" \
    INLOCO_CITIES_ROUTE="" \
    INLOCO_STATES_KEY="" \
    INLOCO_STATES_ID="" \
    INLOCO_STATES_ROUTE="" \
    INLOCO_RS_CITIES_KEY="" \
    INLOCO_RS_CITIES_ROUTE="" \
    GOOGLE_TOKEN=""

ADD ./requirements.txt /app/

RUN pip install -r /app/requirements.txt

# # We need wget to set up the PPA and xvfb to have a virtual screen and unzip to install the Chromedriver
# RUN apt update -y
# RUN apt-get install -y wget xvfb unzip

# # Set up the Chrome PPA
# RUN wget --no-check-certificate -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
# RUN echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list

# # Update the package list and install chrome
# RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
# RUN dpkg -i google-chrome-stable_current_amd64.deb; apt-get -fy install

# # Set up Chromedriver Environment variables
# ENV CHROMEDRIVER_VERSION 2.19
# ENV CHROMEDRIVER_DIR /chromedriver
# RUN mkdir $CHROMEDRIVER_DIR

# # Download and install Chromedriver
# RUN wget -q --continue -P $CHROMEDRIVER_DIR "http://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip"
# RUN unzip $CHROMEDRIVER_DIR/chromedriver* -d $CHROMEDRIVER_DIR

# # Put Chromedriver into the PATH
# ENV PATH $CHROMEDRIVER_DIR:$PATH

# install google chrome
RUN apt update -y
RUN wget --no-check-certificate -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable

# install chromedriver
RUN apt-get install -yqq unzip
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

# set display port to avoid crash
ENV DISPLAY=:99

COPY ./src/loader /app/src/

RUN chmod +x /app/src/entrypoint.sh

ENTRYPOINT /app/src/entrypoint.sh

# HACK convert UWSGI container to one-time runner
RUN rm main.py prestart.sh uwsgi.ini

WORKDIR /app/src
