# SimulaCovid API
---

This repo runs our API for [SimulaCovid](simulacovid.coronacidades.org). You can access the API here: http://datasource.coronacidades.org:7000/v1/raw/csv


## Building your local API
---

⚠️ *You need a file in `src/loader/secret.yaml` with
very secretive variables.*


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


You should see something at `localhost:7000/<endpoint>`, like `http://localhost:7000/v1/raw/csv` 

> Check the column `date_last_refreshed` if you made any changes! ;)

## Analysis
---

To make some drafts on `notebooks` folder, start the venv
```
make loader-create-env-analysis
```