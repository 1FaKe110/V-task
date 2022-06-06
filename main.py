import logging
import os
import sys

from variables import *
import smev_register_checker as src
import psycopg2

logging.basicConfig(format='%(asctime)s | %(levelname)s | %(message)s', filename='SmevRegisterCheck.log', filemode='w', level=logging.DEBUG)
logging.getLogger(__name__)


class SmevRegisterCheck:

    def __init__(self, circuit, db_type='postgresql', db_name='rosreestr_checks'):
        self.__circuit = circuit
        self.__db_type = db_type
        self.__db_name = db_name

        self.__reply = None

    def __get_stats_from_db_data__(self, select):

        cd = db_connection_data[self.__circuit][self.__db_type][self.__db_name]

        try:
            connection = psycopg2.connect(user=cd['username'],
                                          password=cd['password'],
                                          host=cd['server'],
                                          port=cd['port'],
                                          database=cd['database'])
            cursor = connection.cursor()

            cursor.execute(select)
            reply = cursor.fetchall()

            data = []
            """
                "request_id": "wait-pass",
                "request_date": "2022-05-26T08:10:03.815Z",
                "response_ack": false,
                "reject_id": "20222222"
            """
            for row in reply:
                adds = {
                    'request_id': row[0],
                    'request_date': row[1],
                    'response_ack': row[2],
                    'reject_id': row[3],
                }
                data.append(adds)

            self.__reply = data

        except (Exception, psycopg2.Error) as error:
            logging.error("Error while fetching data from PostgreSQL", error)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                logging.debug("PostgreSQL connection is closed")

    def __send_answer__(self, data: dict = None):
        """
        send data validator
        :param data: only for testing
        :return:
        """

        if data is None:
            ch = src.Checker(self.__reply, self.__circuit)
            ch.check()
            return

        logging.debug("testing data validator")
        ch = src.Checker(data, self.__circuit)
        ch.check()

    def operate(self, select):
        logging.info("operate")
        logging.debug(f"select - {select}")
        self.__get_stats_from_db_data__(select)

        logging.debug(f'reply - {self.__reply}')

        if self.__reply is None:
            sys.exit("no reply")

        self.__send_answer__()


def main():
    cur_dir = os.popen('pwd').read().strip("\n")
    logs_path = f'{cur_dir}/SmevRegisterCheck.log'
    print(f"logs at {logs_path}")

    src_handler = SmevRegisterCheck(ui_circuit)
    select = "SELECT " \
             "inbox.request_id, inbox.request_date, documents.response_ack, documents.reject_id " \
             "FROM " \
             "public.documents documents, public.inbox inbox " \
             "where " \
             "documents.inbox_id = inbox.id " \
             "and inbox.request_date < '2022-06-03';"
    src_handler.operate(select)
    print('done')


if __name__ == '__main__':
    main()
