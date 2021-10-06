import logging
import azure.functions as func
import os
import mysql.connector
from mysql.connector import errorcode
from azure.storage.blob import BlobServiceClient

config = {
  'host':'',
  'user':'',
  'password':'',
  'database':'',
  'client_flags': [mysql.connector.ClientFlag.SSL],
  'ssl_ca': './DigiCertGlobalRootG2.crt.pem'
}

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # auth token

        logging.info('Python HTTP trigger function processed a request.')

        # get a request param
        method = req.method
        model_name = req.files['model_name']
        model_id = req.params.get('model_id')
        upload_user = req.params.get('upload_user')
        model_info_cosmos_address = req.params.get('model_info_cosmos_address')
        container_name = req.params.get('blob_address')

        if method != 'POST':
            logging.warning(f'Application error')
            return func.HttpResponse(f'only accept POST method', status_code=400)

        # connect to blob

        connect_str = 'Your connect str goes here'
        service = BlobServiceClient.from_connection_string(connect_str)

        service.create_container(container_name)

        # rename files
        model_basename = os.path.basename(model_name)
        model_format = os.path.splitext(model_name)
        model_rename = model_basename + model_format

        # upload to blob

        blob_client = service.get_blob_client(container=container_name, blob=model_rename)

        logging.info("\nUploading to Azure Storage as blob:\n\t {}".format(model_rename))

        with open(model_rename, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        logging.info("Done!")
        # connect Azure mySQL

        try:
            conn = mysql.connector.connect(**config)
            logging.info("Connection established")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                return func.HttpResponse("Something is wrong with the user name or password", err.errno)
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                return func.HttpResponse("Database does not exist", status_code=err.errno)
            else:
                logging.error(err)
        else:
            cursor = conn.cursor()

        # Drop previous table of same name if one exists
        cursor.execute("DROP TABLE IF EXISTS inventory;")
        logging.info("Finished dropping table (if existed).")

        # Create table
        cursor.execute("CREATE TABLE model (id int PRIMARY KEY auto_increment, model_name VARCHAR(25), model_id VARCHAR(25), upload_user VARCHAR(10), upload_user VARCHAR(50) blob_address VARCHAR(25));")
        logging.info("Finished creating table.")

        # Insert some data into table
        cursor.execute("INSERT INTO inventory ({}, {}, {}, {}, {}) VALUES (%s, %s);".format(model_name, model_id, upload_user, model_info_cosmos_address, container_name))
        logging.info("Inserted",cursor.rowcount,"row(s) of data.")

        # disconect mySQL
        conn.commit()
        cursor.close()
        conn.close()
        return func.HttpResponse("Done", status_code=200)

    except Exception as e:
        logging.error(e)
        return func.HttpResponse(f'Service Error.check the log.', status_code=500)
    except:
        return func.HttpResponse('Application error', status_code=400)

