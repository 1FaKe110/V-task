import json
import logging

import kafka_sender as ks
from json import dumps as json_dumps
from os import getlogin

from variables import ui_skip_kafka as ui_sk
from variables import kafka_topic as topic
from variables import srv


class Checker:

    def __init__(self, data, circuit):
        self.__data = data
        self.__circuit = circuit
        self.__srv = srv[circuit]
        self.logger = logging.getLogger('__main__')

    def check(self):

        self.logger.debug(f'data length {len(self.__data)}')
        producer = ks.set_kafka_producer(self.__srv, getlogin())

        for i, row in enumerate(self.__data):

            if row['response_ack'] and row['reject_id'] is None:  # если в поле response_ack значение true и в поле reject_id значение NULL, то проверка пройдена
                d_out = {"requestId": row['request_id'], "journal": [{"severity": "OK", "code": "0400001", "message": "Проверка прошла"}]}
                js_out = json_dumps(d_out, ensure_ascii=False).encode('utf8')

                self.logger.debug(f'json [ {d_out} ]')
                self.logger.info(f"data[{i}] sending json to kafka")

                try:
                    if ui_sk:
                        self.logger.debug(f"'topic {topic}'\n {d_out}\n {producer}")
                        self.logger.debug(f"success [skip]")
                        continue

                    self.logger.debug(f"'topic {topic}'\n {d_out}\n {producer}")
                    ks.send_to_kafka(topic, js_out, producer)
                    self.logger.debug(f"success")

                except Exception as e:
                    self.logger.error(e)

                continue

            elif row['response_ack'] and row['reject_id'] is not None:  # если в поле response_ack значение true и в поле reject_id значение не NULL, то проверка не пройдена
                d_out = {"requestId": "32393-319-181289a397f", "journal": [{"severity": "ERROR", "code": "0400001", "message": "Проверка не прошла"}]}
                js_out = json_dumps(d_out, ensure_ascii=False).encode('utf8')

                self.logger.debug(f'json [ {d_out} ]')
                self.logger.info(f"data[{i}] sending json to kafka")

                try:
                    if ui_sk:
                        self.logger.debug(f"'topic {topic}'\n {d_out}\n {producer}")
                        self.logger.debug(f"success [skip]")
                        continue

                    self.logger.debug(f"'topic {topic}'\n {d_out}\n {producer}")
                    ks.send_to_kafka(topic, js_out, producer)
                    self.logger.debug(f"success")
                except Exception as e:
                    self.logger.error(e)

                continue

            elif not row['response_ack']:  # если в поле response_ack значение false, то ответ по проверке ещё не получен - json слать не нужно.

                self.logger.debug(f"no json because - [{row['response_ack']}] in data[{i}]")
                self.logger.info("skip json sending")
                continue

            self.logger.error(f"[{row['response_ack']}] [{row['reject_id']}] data[{i}]")


def main():
    with open('./reply.json') as f:
        file = f.read()
        data = json.loads(file)

    ch = Checker(data, 'test')
    ch.check()


if __name__ == '__main__':
    main()
