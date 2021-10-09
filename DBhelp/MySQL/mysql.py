import logging
import azure.functions as func
import mysql.connector
from mysql.connector import errorcode
from DBhelp.MySQL.mysql_conf import config as myconf

class Mysql():
    def __init__(self):
        try:
            self.conn = mysql.connector.connect(**myconf)
            logging.info("Connection established")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                return func.HttpResponse("Something is wrong with the user name or password", err.errno)
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                return func.HttpResponse("Database does not exist", status_code=err.errno)
            else:
                logging.error(err)
        else:
            self.cursor = self.conn.cursor()

    def insert(self, model_name, model_id, upload_user, model_info_cosmos_address, container_name):
            # Insert some data into table
            self.cursor.execute("INSERT INTO inventory ({}, {}, {}, {}, {}) VALUES (%s, %s);".format(model_name, model_id, upload_user, model_info_cosmos_address, container_name))
            logging.info("Inserted",self.cursor.rowcount,"row(s) of data.")

    def disconnect(self):
        # disconnect mySQL
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
        return func.HttpResponse("Done", status_code=200)