# Copyright 2021 TradersOfTheUniverse S.A. All Rights Reserved.
#
# [Generic functions and imports related to Redis]
# #
# Authors:
#   antoniojose.luqueocana@telefonica.com
#   joseluis.roblesurquiza@telefonica.com
#   franciscojavier.gonzalezfernandez1@telefonica.com
#
# Version: 0.1
#
from google.cloud import datastore


class DataStoreClient:
    """Clase creada para gestionar la interaccion DS"""

    def __init__(self):

        """ Initialize variables """
        self.__ds_client = datastore.Client()

    def get_entity(self, entity_name: str, key: str):
        """Obtiene una entidad de DS si existe"""

        key = self.__ds_client.key(entity_name, key)
        return self.__ds_client.get(key)

    def update_entity(self, entity: object):
        """Actualiza una entidad de DS"""
        self.__ds_client.put(entity)


# # Local Testing
# if __name__ == "__main__":
#
#     ds_client = DataStoreClient()
#
#     config = ds_client.get_entity("AnalyzerJ3Config", "1m_config")
#
#     config["BetMode"] = "NORMAL_BET"
#     config["Updated"] = datetime.datetime.utcnow()
#
#     ds_client.update_entity(config)
#     print(config)
