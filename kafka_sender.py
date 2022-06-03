import logging
from random import randrange as rr
import confluent_kafka as ck
import logging

logging.getLogger('root')


def set_kafka_producer(srv: str, client_id: str = None, message_size: int = None):
    """
    send json to topic
    :param srv: circuit required
    :param client_id: username
    :param message_size: 10485760 B default
    """

    if message_size is None:
        message_size = 10485760  # equals 10Mb

    if client_id is None:
        client_id = 'test-user'

    conf = {
        'bootstrap.servers': srv,
        'client.id': client_id,
        'message.max.bytes': message_size,  # max sending bytes value
        'receive.message.max.bytes': message_size + 1024  # if kafka sends back message -> max available received bytes + some bytes for protocols and etc.
    }
    logging.debug(f"producer conf:\n {conf}")

    producer_ = ck.Producer(conf)
    return producer_


def send_to_kafka(topic, jsonString, producer):
    """
    send ''json'' to ''topic'' with ''producer''
    :param topic: required str
    :param jsonString: required json
    :param producer: required producer obj
    :return: none
    """

    def keyhold():
        t = 10000
        aint = rr(t, t * t)
        return aint.to_bytes(4, byteorder='big')

    logging.debug(producer.produce(topic, key=keyhold(), value=jsonString))
    producer.flush()
