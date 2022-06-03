import json
from os import listdir
# import psycopg2
import pyodbc


class MssqlConnection:
    def __init__(self, con_data: dict):
        self.connection = self.connect(con_data)
        self.cursor = self.connection.cursor()

    def connect(self, cd: dict):
        driver = 'DRIVER={ODBC Driver 17 for SQL Server};'
        ms = f'PORT=1433;SERVER={cd["server"]}; DATABASE={cd["database"]}; UID={cd["username"]}; PWD={cd["password"]}'
        try:
            connection = pyodbc.connect(driver + ms)
            return connection
        except Exception as e:
            print(e)
            exit(-1)

    def fetchall(self, select):
        try:
            reply = self.cursor.execute(select)
        except Exception as e:
            print(e)
            return -1

        columns = [column[0] for column in reply.description]

        results = []
        for row in reply.fetchall():
            results.append(dict(zip(columns, row)))

        return results

    def close(self):
        self.cursor.close()
        self.connection.close()

    def dump_json(self, data: dict, save_path: str):
        try:
            listdir(save_path)
        except FileNotFoundError:
            print('invalid path!')
            return -1

        js = json.dumps(data)
        with open(f'{save_path}', 'w', encoding='utf-8') as f:
            f.write(js)

        return 0


class PostgreSqlConnection:
    def __init__(self, con_data: dict):
        self.con_data = con_data
        self.connection = self.connect()
        self.cursor = self.connection.cursor()

    def connect(self):
        cd = self.con_data
        try:
            connection = pyodbc.connect(  # psycopg2
                user=cd['username'],
                password=cd['password'],
                host=cd['server'],
                port="5432",
                database=cd['database']
            )
        except Exception as e:
            print(e)
            return -1
        return connection

    def fetchall(self, select):
        try:
            self.cursor.execute(select)
        except Exception as e:
            print(e)
            return -1

        result = self.cursor.fetchall()
        return result

    def close(self):
        self.cursor.close()
        self.connection.close()

    @classmethod
    def dump_json(cls, data: dict, save_path: str):
        try:
            listdir(save_path)
        except FileNotFoundError:
            print('invalid path!')
            return -1

        js = json.dumps(data)
        with open(f'{save_path}', 'w', encoding='utf-8') as f:
            f.write(js)

        return 0
