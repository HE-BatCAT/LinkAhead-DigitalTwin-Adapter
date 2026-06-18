# Digital Twin Adapter

## Requirements

* python. Install requirements: `pip install -r requirements.txt`
* docker (for test setup)

## Test Setup

* Start MQTT Server: `docker compose up -d`

* Start DT Adapter: `./digital_twin/digital_twin.py`
    ```
    INFO:digital_twin_adapter:connect to broker localhost:1883
    INFO:digital_twin_adapter:<pysparkplug._client.Client object at 0x7f6795a9af60> connected
    ```

* Start LinkAhead Adapter: `./linkahead/edge_node.py` (in a different shell)
    ```
    INFO:pysparkplug_builder.builders:connecting to localhost:1883 (tls: False) with user None
    INFO:edge_node_linkahead:publishing metric url_cycling_xlsx=https://www.fileexamples.com/api/sample-file?format=xlsx&size=10485760
    ```

Environment variable for more output: `LOG_LEVEL=DEBUG`.
