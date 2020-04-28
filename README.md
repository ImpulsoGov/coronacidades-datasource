# Test it

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

 ```
 make loader-shell
 make server-shell
 ```
