# stac2resto
Ingest a STAC tree into resto database

## Build application

    docker build -t jjrom/stac2resto:latest .

## Run application

    STAC_DIR=$(pwd)/example
    RESTO_URL=http://host.docker.internal:5252
    docker run -v ${STAC_DIR}:/data -it --rm \
        -e RESTO_URL=${RESTO_URL} \
        -e FORCE_UPDATE=true \
        -e DEVEL=true \
        -e INGEST_STRATEGY=both \
        --add-host=host.docker.internal:host-gateway \
        jjrom/stac2resto:latest /data/catalog.json

