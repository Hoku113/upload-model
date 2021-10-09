import logging
import azure.functions as func
import os
from DBhelp.MySQL.mysql import Mysql
from DBhelp.CosmosDB.cosmosdb import CosmosDB
from Blobhelp.blob import Blob
 


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
        blob_address = req.params.get('blob_address')

        if method != 'POST':
            logging.warning(f'Application error')
            return func.HttpResponse(f'only accept POST method', status_code=400)

        mysql = Mysql()
        blob = Blob(blob_address)
        cosmos = CosmosDB()

        # rename files
        model_name, ext = os.path.splitext(os.path.basename(model_name))
        rename_model = os.rename(model_name, model_id + ext)

        blob.upload(blob_address, rename_model)
        logging.info("Done!")

        mysql.insert(model_name, model_id, upload_user, model_info_cosmos_address, blob_address)

        # disconnect mySQL
        mysql.disconnect()
        return func.HttpResponse("Done", status_code=200)

    except Exception as e:
        logging.error(e)
        return func.HttpResponse(f'Service Error.check the log.', status_code=500)
    except:
        return func.HttpResponse('Application error', status_code=400)

