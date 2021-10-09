import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
from .cosmos_conf import config
from logging import getLogger
logger = getLogger(__name__)

class CosmosDB():
    def __init__(self):
        # Initialize the Cosmos client
        self.client = cosmos_client.CosmosClient(config['ENDPOINT'], {'masterKey': config['master_key']})

        # Read a database
        self.database_link = 'dbs/' + config['DATABASE']
        # self.database_link = self.client.ReadDatabase(self.database_link)['_self']

        # Read a container
        self.container_link = self.database_link + '/colls/{0}'.format(config['CONTAINER'])
        # self.container = self.client.ReadContainer(self.container_link)'_self']


    def get_options(self):
        options = {
            'enableCrossPartitionQuery': True,
            'maxItemCount': 5,
        }
        return options


    # Create a database
    def initialize_database(self, database_id=config['DATABASE']):
        try:        
            return self.client.CreateDatabase({'id': database_id})
            # database_link = self.database_link
            # database_link = db['_self']
        except errors.HTTPFailure as e:
            if e.status_code == 409:
               print('A database with id \'{0}\' already exists'.format(self.database_link))
            else:
                raise errors.HTTPFailure(e.status_code)
        except Exception as e:
            raise e


    # Create a container
    def initialize_container(self, model_name, description, database_id=config['DATABASE']):
        try:
            database_link = 'dbs/' + database_id
            container_definition = {
                "name": model_name,
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
                "description": description
            }
            return self.client.CreateContainer(database_link, container_definition, {'offerThroughput': 400})
        except errors.CosmosError as e:
            if e.status_code == 409:
                logger.error('A collection with id \'{0}\' already exists'.format(self.container_link))
            else:
                raise errors.HTTPFailure(e.status_code) 
        except Exception as e:
            raise e


    # Create and add a item to the container
    def create_item(self, item):
        query = {'query': 'SELECT * FROM c WHERE c.id = "{0}"'.format(item["id"])}

        try:
            self.client.CreateItem(self.container_link, item)
            results = list(self.client.QueryItems(self.container_link, query, self.get_options()))
            for item in results:
                logger.info(item)
            return results
        except errors.HTTPFailure as e:
            if e.status_code == 404:
                print('A collection with id \'{0}\' does not exist'.format(self.container_link))
            elif e.status_code == 409:
                logger.error('A Item with id \'{0}\' already exists'.format(item['id']))            
            else: 
                raise errors.HTTPFailure(e.status_code)
        except Exception as e:
            raise e


    # Delete a item from the container
    def delete_item(self, item):
        query = {'query': 'SELECT * FROM c WHERE c.id = "{0}"'.format(item["id"])}

        try:
            results = self.client.QueryItems(self.container_link, query, self.get_options())
            for item in list(results):
                logger.info(item)
                options = self.get_options()
                options['partitionKey'] = item['partitionKey']
                self.client.DeleteItem(item['_self'], options)
            return results
        except errors.HTTPFailure as e:
            if e.status_code == 404:
                print('A collection with id \'{0}\' does not exist'.format(self.container_link))
            else:
                raise errors.HTTPFailure(e.status_code)
        except Exception as e:
            raise e


    # Upsert a item from the container
    def upsert_item(self, item):
        try:
            result = self.client.UpsertItem(self.container_link, item, self.get_options())
            logger.info(result)
            return result
        except errors.HTTPFailure as e:
            if e.status_code == 404:
                print('A collection with id \'{0}\' does not exist'.format(self.container_link))
            else:
                raise errors.HTTPFailure(e.status_code)
        except Exception as e:
            raise e


    def read_item(self, id):
        query = {'query': 'SELECT * FROM c WHERE c.id = "{0}"'.format(id)}

        try:
            results = list(self.client.QueryItems(self.container_link, query, self.get_options()))
            return results
        except errors.HTTPFailure as e:
            if e.status_code == 404:
                print('A collection with id \'{0}\' does not exist'.format(self.container_link))
            else:
                raise errors.HTTPFailure(e.status_code)
        except Exception as e:
            raise e


    def read_items(self):
        try:
            itemList = list(self.client.ReadItems(self.container_link, {'maxItemCount': 10}))
        
            logger.info('Found {0} documents'.format(itemList.__len__()))
        
            for item in itemList:
                logger.info(item)
            return itemList
        except errors.HTTPFailure as e:
            if e.status_code == 404:
                print('A collection with id \'{0}\' does not exist'.format(self.container_link))
            else:
                raise errors.HTTPFailure(e.status_code)
        except Exception as e:
            raise e


# Sample Data1
def getReplacedItem(id):
    return {
        'id': 'id{0}'.format(id),
        'partitionKey': id,
        'mailaddr': 'test{0}@xxx'.format(id),
        'testaddr' : 'test{0}@xxx'.format(id),
        'message': 'Hello World CosmosDB!',
        'addition': 'test replace {0}'.format(id),
    }
# Sample Data2
def getItem(id):
    return {
        'id': 'id{0}'.format(id),
        'partitionKey': id,
        'mailaddr': 'test{0}@xxx'.format(id),
        'testaddr' : 'test{0}@xxx'.format(id),
        'message': 'Hello World CosmosDB!',
    }