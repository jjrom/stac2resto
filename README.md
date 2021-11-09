# stac2resto
Ingest a STAC tree into resto database

## Build application

    docker build -t jjrom/stac2resto:latest -f Dockerfile.stac2resto .

## Run application

    # Ingest/update collections
    docker run -v /Users/jrom/Devel/stac2resto/example:/data -it --rm --name="stac2resto" -e COLLECTION_DEFAULT_MODEL=MarsPDSSPModel --add-host=host.docker.internal:host-gateway jjrom/stac2resto:latest /data/catalog.json

    # Ingest/update collections AND features
    docker run -v /Users/jrom/Devel/stac2resto/example:/data -it --rm --name="stac2resto" -e COLLECTION_DEFAULT_MODEL=MarsPDSSPModel -e INGEST_FEATURE=true --add-host=host.docker.internal:host-gateway jjrom/stac2resto:latest /data/catalog.json

