# Test it

To run it locally, you need a file in `src/loader/secret.yaml` with
very secretive variables.


Run this to load the files
```
 make loader-build-run
```

In a different tab, run the Flask server

```
make server-build-run
```

You should see something at `localhost:80/<endpoint>`, like
`http://localhost:80/v1/raw/csv`

 # Debug it

Remember to rebuild the docker everytime you change the code.

 ```
 make loader-shell
 make server-shell
 ```
# Analysis


Start env
```
make loader-create-env-analysis
```