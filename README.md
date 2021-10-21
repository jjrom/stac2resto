# stac2resto
Ingest a STAC tree into resto database

## Build application

    docker build -t jjrom/stac2resto:latest -f Dockerfile.stac2resto .

## Run application

    docker run -v /Users/jrom/Devel/stac2resto/example:/data -it --rm --name="stac2resto" jjrom/stac2resto:latest /data/catalog.json http://host.docker.internal:5252