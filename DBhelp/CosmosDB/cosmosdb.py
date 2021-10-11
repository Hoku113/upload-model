from typing import Container
from azure.cosmos import database
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
from .cosmos_conf import config
from logging import getLogger, info
logger = getLogger(__name__)

class CosmosDB():
    def __init__(self):
        # Initialize the Cosmos client
        self.client = cosmos_client.CosmosClient(config['ENDPOINT'], {'PRIMARIY_KEY': config['PRIMARIY_KEY']})

        # Read a database
        self.db = self.client.get_database_client(config['DATABASE'])

        # Read a container
        self.container = self.db.get_container_client(config['CONTAINER'])


    # Create a database
    def create_database(self, database_id=config['DATABASE']):
        try:        
            return self.client.create_database({'id': database_id})
        except errors.CosmosHttpResponseError as e:
            if e.status_code == 409:
               print('A database with id \'{0}\' already exists'.format(self.database_link))
            else:
                raise errors.CosmosHttpResponseError(e.status_code)
        except Exception as e:
            raise e


    # Create a container
    def crate_container(self, container=config['CONTAINER']):
        try:
            return self.db.create_container(container)
        except errors.CosmosResourceExistsError as e:
            if e.status_code == 409:
                logger.error('A collection with id \'{0}\' already exists'.format(self.container_link))
            else:
                raise errors.CosmosHttpResponseError(e.status_code) 
        except Exception as e:
            raise e


    # Create and add a item to the container
    def create_item(self, item):
        query = {'query': 'SELECT * FROM c WHERE c.id = "{0}"'.format(item["id"])}

        try:
            self.container.create_item(item)
            results = list(self.container.query_items(query))
            for item in results:
                logger.info(item)
            return results
        except errors.CosmosHttpResponseError as e:
            if e.status_code == 404:
                print('A collection with id \'{0}\' does not exist'.format(self.container_link))
            elif e.status_code == 409:
                logger.error('A Item with id \'{0}\' already exists'.format(item['id']))            
            else: 
                raise errors.CosmosHttpResponseError(e.status_code)
        except Exception as e:
            raise e


    # Delete a item from the container
    def delete_item(self, item):
        query = {'query': 'SELECT * FROM c WHERE c.id = "{0}"'.format(item["id"])}

        try:
            results = self.container.query_items(self.container_link, query, self.get_options())
            for item in list(results):
                logger.info(item)
                options = self.get_options()
                options['partitionKey'] = item['partitionKey']
                self.client.DeleteItem(item['_self'], options)
            return results
        except errors.CosmosHttpResponseError as e:
            if e.status_code == 404:
                print('A collection with id \'{0}\' does not exist'.format(self.container_link))
            else:
                raise errors.CosmosHttpResponseError(e.status_code)
        except Exception as e:
            raise e


    # Upsert a item from the container
    def upsert_item(self, item):
        try:
            result = self.container.upsert_item(self.container_link, item, self.get_options())
            logger.info(result)
            return result
        except errors.CosmosHttpResponseError as e:
            if e.status_code == 404:
                print('A collection with id \'{0}\' does not exist'.format(self.container_link))
            else:
                raise errors.CosmosHttpResponseError(e.status_code)
        except Exception as e:
            raise e


    def read_item(self, id):
        query = {'query': 'SELECT * FROM c WHERE c.id = "{0}"'.format(id)}

        try:
            results = list(self.client.QueryItems(self.container_link, query, self.get_options()))
            return results
        except errors.CosmosHttpResponseError as e:
            if e.status_code == 404:
                print('A collection with id \'{0}\' does not exist'.format(self.container_link))
            else:
                raise errors.CosmosHttpResponseError(e.status_code)
        except Exception as e:
            raise e


    def read_items(self):
        try:
            itemList = list(self.container.read_all_items({'maxItemCount': 10}))
        
            logger.info('Found {0} documents'.format(itemList.__len__()))
        
            for item in itemList:
                logger.info(item)
            return itemList
        except errors.CosmosHttpResponseError as e:
            if e.status_code == 404:
                print('A collection with id \'{0}\' does not exist'.format(self.container_link))
            else:
                raise errors.CosmosHttpResponseError(e.status_code)
        except Exception as e:
            raise e

# Sample Data2
def getItem(rename_model, description):
    return {
    "name":"model_name",
    "file_type":"h5",
    "precision":"INT8",
    "input":
    {
        "type":"image",
        "channel_order":"bgr",
        "data_order":"hwc",
        "size":
        {
            "weight":"1000",
            "height":"1000"
        }

    },
    "output":
    {
        "type":"image",
        "channel_order":"rgb",
        "size":
        {
            "weight":"1000",
            "height":"1000"
        }
    },
    "description":"this is description"
}