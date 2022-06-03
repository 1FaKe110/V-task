import json
import logging
import kafka_sender as ks
from json import dumps as json_dumps
from os import getlogin

from variables import ui_skip_kafka as ui_sk
from variables import srv

logging.getLogger('root')


class Checker:

    def __init__(self, data, circuit):
        self.__data = data
        self.__circuit = circuit
        self.__srv = srv[circuit]

    def check(self):

        logging.debug(f'data length {len(self.__data)}')
        producer = ks.set_kafka_producer(self.__srv, getlogin())

        for i, row in enumerate(self.__data):

            if row['response_ack'] and row['reject_id'] is None:  # если в поле response_ack значение true и в поле reject_id значение NULL, то проверка пройдена
                d_out = {"requestId": row['request_id'], "journal": [{"severity": "OK", "code": "0400001", "message": "Проверка прошла"}]}
                js_out = json_dumps(d_out)

                logging.debug(f'json [ {js_out} ]')
                logging.info(f"data[{i}] sending json to kafka")

                try:
                    if ui_sk:
                        logging.debug(f"'topic smev-reg-realty-out'\n {js_out}\n {producer}")
                        logging.debug(f"success [skip]")
                        continue

                    ks.send_to_kafka('smev-reg-realty-out', js_out, producer)
                    logging.debug(f"success")

                except Exception as e:
                    logging.error(e)

                continue

            elif row['response_ack'] and row['reject_id'] is not None:  # если в поле response_ack значение true и в поле reject_id значение не NULL, то проверка не пройдена
                d_out = {"requestId": "32393-319-181289a397f", "journal": [{"severity": "ERROR", "code": "0400001", "message": "Проверка не прошла"}]}
                js_out = json_dumps(d_out)

                logging.debug(f'json [ {js_out} ]')
                logging.info(f"data[{i}] sending json to kafka")

                try:
                    if ui_sk:
                        logging.debug(f"'topic smev-reg-realty-out'\n {js_out}\n {producer}")
                        logging.debug(f"success [skip]")
                        continue

                    ks.send_to_kafka('smev-reg-realty-out', js_out, producer)
                    logging.debug(f"success")
                except Exception as e:
                    logging.error(e)

                continue

            elif not row['response_ack']:  # если в поле response_ack значение false, то ответ по проверке ещё не получен - json слать не нужно.

                logging.debug(f"no json because - [{row['response_ack']}] in data[{i}]")
                logging.info("skip json sending")
                continue

            logging.error(f"[{row['response_ack']}] [{row['reject_id']}] data[{i}]")


def main():
    with open('./reply.json') as f:
        file = f.read()
        data = json.loads(file)

    ch = Checker(data, 'test')
    ch.check()


if __name__ == '__main__':
    main()
