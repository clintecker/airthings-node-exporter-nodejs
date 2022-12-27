# Node Exports for Airthings

## How to Install

```shell
$> yarn
```

## How To Run The Server

```shell
$> AIRTHINGS_CLIENT_ID=0000-0000-000-000-0000 \
AIRTHINGS_CLIENT_SECRET=0000-0000-000-0000-000-0000 \
ACCESS_TOKEN_PATH=./token.json \
LATEST_SAMPLES_PATH=./samples.json \
AIRTHINGS_CLIENT_SCOPE=read:device:current_values \
CACHE_LATEST_SAMPLES_FOR=300 \
LISTEN_PORT=9099 \
  node index.js
```

Once you've got your server running you can point prometheus at the IP of the machine running the server on the port you selected.
