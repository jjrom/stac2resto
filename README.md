# stac2resto
Ingest a STAC tree into resto database

## Build application

    docker build -t jjrom/stac2resto:latest -f Dockerfile.stac2resto .

## Run application

    docker run -v /Users/jrom/Devel/stac2resto/example:/data -it --rm --name="stac2resto" -e COLLECTION_DEFAULT_MODEL=MarsModel jjrom/stac2resto:latest /data/catalog.json

    docker run -v /home/ubuntu/stac2resto/example:/data -it --rm --name="stac2resto" -e RESTO_ADMIN_AUTH_TOKEN=eyJzdWIiOiIxMDAiLCJpYXQiOjE2MzY0NjMyNTEsImV4cCI6MjUwMDQ2MzI1MX0.WrzQqNrJnQoFFr50GhZZzUVEikPJbR7WBo_-pzzq3pc -e COLLECTION_DEFAULT_MODEL=MarsPDSSPModel jjrom/stac2resto:latest /data/catalog.json
    