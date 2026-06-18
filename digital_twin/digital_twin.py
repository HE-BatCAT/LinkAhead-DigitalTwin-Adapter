#!/usr/bin/env python

import os
import time
import logging
from urllib.request import urlretrieve
from concurrent.futures import ThreadPoolExecutor, Executor
import pysparkplug as psp
from pysparkplug_builder import SparkplugGroup, settings as sparkplug_settings

LOG_LEVEL=os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("digital_twin_adapter")

group = SparkplugGroup()
server_id = group.default_server_id


class Broker:
    host = group.servers[server_id].host
    port = group.servers[server_id].port
    keepalive = 60


class Settings:

    client_id = "digital-twin"
    transport_config = psp.TLSConfig(**group.servers[server_id].tls_config.model_dump()) if group.servers[server_id].tls_config is not None else None
    broker = Broker()
    retry = True

    group_id = group.group_id

    # linkahead
    edge_node = group.edge_nodes["edge_node_linkahead"]
    cycling_data_source = edge_node.devices["cycling_data_source"]
    url_metric = cycling_data_source.metrics["url_cycling_xlsx"]

    cycling_decision_sink = edge_node.devices["cycling_decision_sink"]
    decision_metric = cycling_decision_sink.metrics["cycling_decision"]

    username = sparkplug_settings.sparkplug_username
    password = sparkplug_settings.sparkplug_password


settings = Settings()

class DigitalTwin:

    def _request_decision(self, local_path_to_cycling_data):
        logger.info("######## Processing: %s", local_path_to_cycling_data)
        # do magic
        return "this is the decision"

    def _download(self, url):
        logger.info("######## Downloading: %s", url)
        local_path, msg = urlretrieve(url)
        logger.debug(msg)
        return local_path

    def request_decision(self, url):
        local_path = self._download(url)
        return self._request_decision(local_path)

class DigitalTwinFacade:

    def __init__(self, mqtt_client: psp.Client, executor: Executor, priority_executor: Executor | None = None):
        self.mqtt_client = mqtt_client
        self.executor = executor
        self.priority_executor = priority_executor if priority_executor is not None else executor

    def process_sensor_data(self, client: psp.Client, message: psp.Message) -> None:
        self.executor.submit(self._process_sensor_data, client, message)

    def _get_metric_by_name(self, message: psp.Message, name: str) -> psp.Metric:
        for metric in message.payload.metrics:
            if metric.name == name:
                return metric

    def _process_sensor_data(self, client: psp.Client, message: psp.Message) -> None:
        logger.debug(message)
        if isinstance(message.payload, (psp.DData)):
            metric = self._get_metric_by_name(message, settings.url_metric.name)
            if metric is not None and not metric.is_null:

                dt = DigitalTwin()
                decision = dt.request_decision(url=metric.value)
                self.trigger_actuator(decision=decision)

    def trigger_actuator(self, decision: str):
        self.priority_executor.submit(self._trigger_actuator, decision)

    def _trigger_actuator(self, decision: str):
        logger.info("######## Send command: %s=%s", settings.decision_metric.name, decision)

        try:
            metrics = (
                psp.Metric(
                    timestamp=psp.get_current_timestamp(),
                    name=settings.decision_metric.name,
                    datatype=psp.DataType.STRING,
                    value=decision,
                ),
            )
            payload = psp.DCmd(timestamp=psp.get_current_timestamp(), metrics=metrics)

            topic = psp.Topic(
                message_type=psp.MessageType.DCMD,
                group_id=settings.group_id,
                edge_node_id=settings.edge_node.edge_node_id,
                device_id=settings.cycling_decision_sink.device_id
            )
            self.mqtt_client.publish(
                psp.Message(
                    topic=topic,
                    payload=payload,
                    qos=psp.QoS.AT_MOST_ONCE,
                    retain=False,
                ),
                include_dtypes=True,
            )
        except Exception as e:
            logger.error(e)

def retry(n=0, on=(Exception), sleep=0):
    """Retry decorator.

    Retry the decorated functio N times on the given exceptions.

    If no exception or the wrong exception is being thrown the function terminates without any further
    retries.

    Parameters:
    -----------
    n : int, optional, default=0
        The number of times to repeat the decorated function. 0 means the function runs once (no retry). -1
        means the function is being retried forever.
    on : tuple of Exception, optional, default=(Exception)
        The exception which trigger a retry.
    sleep : float, optional, default=0.0
        Sleep between retries. A value of zero or smaller will result in no sleep time between retries.
    """
    if n < -1:
        raise ValueError("n must be >= -1")
    elif n == -1:
        log_msg = "retry %s, %d"
    else:
        log_msg = f"retry %s, %d of {n}"
    if sleep > 0:
        log_msg += f", after {sleep}s"

    def decorator(func):
        def wrapper(*args, **kwargs):
            counter = 0
            while counter < n or n == -1:
                try:
                    return func(*args, **kwargs)
                except on:
                    logger.warning(log_msg, func.__name__, counter)
                    counter += 1
                    if sleep > 0:
                        time.sleep(sleep)
            return func(*args, **kwargs)
        return wrapper
    return decorator


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client: psp.Client) -> None:
    logger.info(f"{client} connected")


with ThreadPoolExecutor() as executor:
    mqttc = psp.Client(settings.client_id, transport_config=settings.transport_config,
                       username=settings.username, password=settings.password)

    digital_twin = DigitalTwinFacade(mqtt_client=mqttc, executor=executor)

    def on_message(client: psp.Client, message: psp.Message):
        digital_twin.process_sensor_data(client=client, message=message)

    # subscribe to linkahead
    topic = psp.Topic(group_id=settings.group_id,
                      message_type="+",
                      edge_node_id=settings.edge_node.edge_node_id,
                      device_id=settings.cycling_data_source.device_id)
    mqttc.subscribe(topic=topic, qos=psp.QoS.AT_MOST_ONCE,
                    callback=on_message)


    n = -1 if settings.retry is True else settings.retry

    @retry(n=n, on=(ConnectionRefusedError), sleep=1)
    def connect(**kwargs):
        mqttc.connect(**kwargs)

    logger.info("connect to broker %s:%d", settings.broker.host, settings.broker.port)
    try:
        connect(host=settings.broker.host, port=settings.broker.port, callback=on_connect, blocking=True)
    except KeyboardInterrupt:
        pass
    finally:
        mqttc.disconnect()
        logger.info("MQTT disconnect")
