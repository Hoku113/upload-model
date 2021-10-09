from azure.storage.blob import BlobServiceClient

class Blob():
    def __init__(self, blob_address):
        connect_str = 'Your connect str goes here'
        self.client = BlobServiceClient.from_connection_string(connect_str)
        self.client.create_container(blob_address)

    def upload(self, blob_address, rename_model):
        blob_client = self.client.get_blob_client(container=blob_address, blob=rename_model)

        with open(rename_model, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)