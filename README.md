# Coronacidades API 🎲 

<p align="left">
 <a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
 <a href="https://github.com/ImpulsoGov/coronacidades-datasource/graphs/contributors"><img alt="Contributors" src="https://img.shields.io/github/contributors/ImpulsoGov/coronacidades-datasource"></a>
 <a href=""><img alt="Update master" src="https://img.shields.io/github/last-commit/ImpulsoGov/coronacidades-datasource/master?label=last%20updated%20%28master%29"></a>
 <a href="https://hub.docker.com/repository/docker/impulsogov/coronacidades-datasource-server"><img alt="docker build server" src="https://img.shields.io/docker/cloud/build/impulsogov/coronacidades-datasource-server?label=docker%20build%20server"></a>
 <a href="https://hub.docker.com/repository/docker/impulsogov/coronacidades-datasource-loader"><img alt="docker build loader" src="https://img.shields.io/docker/cloud/build/impulsogov/coronacidades-datasource-loader?label=docker%20build%20loader"></a>
</p>

This repo runs our API for Coronacidades tools, such as [FarolCovid](farolcovid.coronacidades.org). You can access the API here: http://datasource.coronacidades.org/help

## Current data

Use it like:

`http://datasource.coronacidades.org/br/cities/cases/full`

- `br/cities/cases/full`:	Full history data from Brasil.IO with notification rate and estimated active cases
- `br/cities/cnes`:	Beds and ventilators data from DataSus/CNES
- `br/cities/farolcovid/main`:	Data filtered & cities'indicatores for FarolCovid app
- `br/cities/rt`:	Cities effective reproduction number (Rt) calculations by date
- `br/cities/simulacovid/main`:	Data filtered to serve SimulaCovid app
- `br/states/farolcovid/main`:	Data filtered & states' indicators for FarolCovid app
- `br/states/rt`:	State effective reproduction number (Rt) calculations by date
- `br/states/safereopen/main`: States' security and economic priority index for reopening sectors
- `world/owid/heatmap`:	Our World in Data data to serve the heatmaps

**[Check data dictionaries here](/dictionaries).**


## Building your local API

⚠️ *You need a file in `src/loader/secrets/secrets.yaml` and another in `src/loader/secrets/token.pickle` with very secretive variables to run some data.*

### 1️⃣ Run Loader 

Run the code to load the files

```bash
 make loader-build-run
```

#### For development

If you want to make changes on the code, you should run the loader with `make loader-dev` to open the docker image and be able to edit the files directly in your editor.


### 2️⃣ Run Server

*In a different tab*, run the Flask server

```bash
make server-build-run
```

#### For development

If you want to make changes on the code, you should run the loader with `make server-dev` to open the docker image and be able to edit the files directly in your editor.

### 3️⃣ All done!
You should see something at `localhost:7000/<endpoint>`, like `http://localhost:7000/br/cities/cases/full` 

> Check the column `date_last_refreshed` if you made any changes! ;)

## Adding new data entrypoints


- Add the endpoint configuration parameters to `endpoints.yaml`

```yaml
- endpoint: 'br/cities/cases/full' # endpoint route following [country]/[unit]/[content]
  python_file: get_cases         # .py that generates data in loader/endpoints/
  update_frequency_minutes: 15  # how often it should be updated in minutes
```

- Write a .py file in loader/endpoints/ that generates the endpoint data. **This file must be structured as [get_{template}.py](/src/loader/endpoints/get_{template}.py)**


## Good Practices

### Logging

Do not use `print` to log stuff. You can use the logger in `logger.py`. 
It is faily simple to do it.

```python
from logger import logger

logger.debug('Your message {variable}', variable=3)
```

You have several logging levels: debug, info, warning and error. To change the level 
you can just call `logger.<level>`, i.e., `logger.info()`.

- Usually, `debug` is used to minor behaviours and it is useful to debug code :).

- The `info` level is being used to keep track of the overall expected behaviour.

- If you use `logger.error`, the message will also be posted to our slack channel **#simulacovid-logs**

Read more at [loguru](https://github.com/Delgan/loguru).

### Production: print logs on Slack

1. Set the env variable `IS_PROD=True`
2. Add secrets folder
