# stac2resto
Ingest a STAC tree into resto database

## Build application

    docker build -t jjrom/stac2resto:latest -f Dockerfile.stac2resto .

## Run application

    docker run -v /Users/jrom/Devel/stac2resto/example:/data -it --rm --name="stac2resto" -e COLLECTION_DEFAULT_MODEL=MarsModel --add-host=host.docker.internal:host-gateway jjrom/stac2resto:latest /data/catalog.json
