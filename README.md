# SimulaCovid API

<p align="left">
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a> <a href="https://github.com/ImpulsoGov/simulacovid-datasource/graphs/contributors"><img alt="Contributors" src="https://img.shields.io/github/contributors/ImpulsoGov/simulacovid-datasource"></a> <a href=""><img alt="Contributors" src="https://img.shields.io/github/last-commit/ImpulsoGov/simulacovid-datasource/master?label=last%20updated%20%28master%29"></a>
</p>



This repo runs our API for [SimulaCovid](simulacovid.coronacidades.org). You can access the API here: http://datasource.coronacidades.org/help

## Current data

- `br/cities/cases/full`: full history data from Brasil.IO with notification rate and estimated active cases
- `br/states/rt`: State Rt calculations
- `br/cities/rt`: Cities Rt calculations
- `br/cities/embaixadores`: beds and ventilators data updated by SimulaCovid's ambassadors
- `br/cities/cnes`: beds and ventilators data from Data SUS/CNES
- `br/cities/farolcovid/main`: data filtered for cities to serve FarolCovid app
- `br/states/farolcovid/main`: data filtered for states serve FarolCovid app
- `br/cities/simulacovid/main`: data filtered to serve SimulaCovid app
- `world/owid/heatmap`: Our World in Data data to serve the heatmaps


## Building your local API

⚠️ *You need a file in `src/loader/secrets/secrets.yaml` and another in `src/loader/secrets/token.pickle` with very secretive variables.*


1️⃣ Run the code to load the files
```
 make loader-build-run
```

> **Remember to rebuild the docker everytime you change the loader folder!** 
> If the changes have any errors, running `make loader-build-run` will just ignore your changes and get the previous stable version. So, if you change any file on `loader` folder, instead run:
>
> ```shell
> >>> make loader-shell # open a terminal on loader container
> >>> root@blabla:/app python3 main.py
> ```


2️⃣ In a different tab, run the Flask server

```bash
make server-build-run
```

> **Remember to rebuild the docker everytime you change the server folder!** 
> If the changes have any errors, running `make server-build-run` will just ignore your changes and get the previous stable version. So, if you change any file on `server` folder, instead run:
>
> ```bash
> >>> make server-shell # open a terminal on server container
> >>> root@blabla:/app python3 main.py
> ```


You should see something at `localhost:7000/<endpoint>`, like `http://localhost:7000/br/cities/cases/full` 

> Check the column `date_last_refreshed` if you made any changes! ;)

## Adding new data entrypoints

1️⃣ Add the endpoint configuration parameters to `endpoints.yaml`

```yaml
- endpoint: 'br/cities/cases/full' # endpoint route following [country]/[unit]/[content]
  python_file: get_cases         # .py that generates data in loader/endpoints/
  update_frequency_minutes: 15  # how often it should be updated in minutes
```

2️⃣ Write a .py file in loader/endpoints/ that generates the endpoint data. **This file must be structured as [get_{template}.py](/src/loader/endpoints/get_{template}.py)**

## Analysis

To make some drafts on `notebooks` folder, start the venv
```
make loader-create-env-analysis
```

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
