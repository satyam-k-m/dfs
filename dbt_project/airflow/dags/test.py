from azure.storage.blob import BlobServiceClient
CONTAINER_NAME = "abcd"
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dfsaassdpnprdadls01;AccountKey=t6mFmpdIM/FvC8BwIV87TySyAhXOLSGS1qxTadvafil9L2sX5N9dIUi+S6I6bqXu8+gV6kkazgZO+AStCQaVxg==;EndpointSuffix=core.windows.net"
BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(CONNECTION_STRING)
CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client(CONTAINER_NAME)


print(CONTAINER_CLIENT.get_blob_list())