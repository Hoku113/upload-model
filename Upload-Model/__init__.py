import logging
import azure.functions as func
import os
from azure.storage.blob import BlobServiceClient


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


        # connect Azure mySQL

        # connect to blob

        connect_str = 'Your connect str goes here'
        service = BlobServiceClient.from_connection_string(connect_str)

        container_client = service.create_container(container_name)

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
        # register to mySQL

        # disconect mySQL

        if not upload_user:
            try:
                req_body = req.get_json()
            except ValueError:
                pass
            else:
                name = req_body.get('upload_user')

        if name:
            return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
        else:
            return func.HttpResponse(
                "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.", status_code=200)

    except Exception as e:
        logging.error(e)
        return func.HttpResponse(f'Service Error.check the log.', status_code=500)
    except:
        return func.HttpResponse('Application error', status_code=400)

