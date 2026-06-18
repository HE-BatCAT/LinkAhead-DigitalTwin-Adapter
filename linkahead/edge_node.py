#!/usr/bin/env python

import sys
import os
import time
import random
import logging
import pysparkplug as psp
from pysparkplug_builder import SparkplugGroup

LOG_LEVEL=os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("edge_node_linkahead")

group = SparkplugGroup()
edge_node_builder = group.edge_nodes["edge_node_linkahead"]
edge_node = edge_node_builder.build()

# Connect to broker
edge_node.connect()

url_metric_builder = edge_node_builder.devices["cycling_data_source"].metrics["url_cycling_xlsx"]
decision_metric_builder = edge_node_builder.devices["cycling_decision_sink"].metrics["cycling_decision"]

@edge_node_builder.devices["cycling_decision_sink"].listener("cycling_decision", psp.MessageType.DCMD)
def handle_switch_command(value):
    logger.info("######## Received command: %s=%s", decision_metric_builder.name, value)

sleep_for = 10
if len(sys.argv) > 1:
    sleep_for = int(sys.argv[1])

try:
    while True:
        time.sleep(sleep_for)
        next_value="https://www.fileexamples.com/api/sample-file?format=xlsx&size=10485760"
        logger.info("publishing metric %s=%s", url_metric_builder.name, next_value)
        metrics = [
                url_metric_builder.build_value(next_value)
        ]

        # send data
        edge_node.update_device("cycling_data_source", metrics)


    # clean up
except KeyboardInterrupt:
    pass
finally:
    edge_node.deregister("cycling_data_source")
    edge_node.deregister("cycling_decision_sink")
    edge_node.disconnect()
