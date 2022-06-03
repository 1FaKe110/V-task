import json

import logging
from os import popen
from variables import *
import smev_register_checker as src

logging.basicConfig(format='%(asctime)s | %(levelname)s | %(message)s', filename='SmevRegisterCheck.log', filemode='w', level=logging.DEBUG)


class SmevRegisterCheck:

    def __init__(self, circuit, db_type='postgresql', db_name='rosreestr_checks'):
        self.__circuit = circuit
        self.__db_type = db_type
        self.__db_name = db_name

        self.__reply = None

    def __get_stats_from_db_data__(self, select):
        from DBConnections import PostgreSqlConnection as PG

        connection_data = db_connection_data[self.__circuit][self.__db_type][self.__db_name]

        logging.debug(f'connect to db \n {connection_data}')
        pg_con = PG(connection_data)

        self.__reply = pg_con.fetchall(select)
        logging.debug(f"select - {select}")

        pg_con.close()
        pg_con.dump_json(self.__reply, f"{popen('pwd').read()}/db_reply.json")
        logging.debug(f"save to {popen('pwd').read()}/db_reply.json")

    def __send_answer__(self, data: dict = None):
        """
        send data validator
        :param data: only for testing
        :return:
        """

        if data is None:
            src.Checker(self.__reply, self.__circuit)
            return

        logging.debug("testing data validator")
        src.Checker(data, self.__circuit)

    def operate(self, select):
        logging.info("operate")
        self.__get_stats_from_db_data__(select)
        self.__send_answer__()


def main():
    src_handler = SmevRegisterCheck(ui_circuit)
    select = "SELECT " \
             "inbox.request_id, inbox.request_date, documents.response_ack, documents.reject_id " \
             "FROM " \
             "public.documents documents, public.inbox inbox " \
             "where " \
             "documents.inbox_id = inbox.id " \
             "and inbox.request_date < '2022-06-03';"
    src_handler.operate(select)


def test():
    logging.debug("testing")
    src_handler = SmevRegisterCheck(ui_circuit)

    with open('./reply.json') as f:
        file = f.read()
        data = json.loads(file)

    logging.debug(f"test reply:\n{data}")
    src_handler.__send_answer__(data)


if __name__ == '__main__':
    test()
