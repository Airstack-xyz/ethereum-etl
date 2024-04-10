import os
import clickhouse_connect
import logging

class Clickhouse:
    """
    Clickhouse Connector
    """

    def __init__(self):
        """
        Connect to database and provide it's client
        :param host:
        :param port:
        :param username:
        :param password:
        :param database:
        """
        logging.debug('Connecting to clickhouse !!')
        
        self._host = os.environ['CLICKHOUSE_HOST']
        self._port = os.environ['CLICKHOUSE_PORT']
        self._username = os.environ['CLICKHOUSE_USERNAME']
        self._password = os.environ['CLICKHOUSE_PASSWORD']
        self._database = os.environ['CLICKHOUSE_DATABASE']
   
        logging.debug(
            f'Making Connection to DB with host: {self._host}, port: {self._port}, database: {self._database}'
        )
        self._connection = clickhouse_connect.get_client(host=self._host, port=self._port, 
                                                         username=self._username, password=self._password, database=self._database)        


    async def run_query(self, query, parameters):
        """Function to run query on clickhouse

        Args:
            query (str): query to run
            parameters (dict): variable parameters

        Returns:
            list: fetched data
        """
        logging.debug(f'Running SQL Query {query}')
        result = self._connection.query(query=query, parameters=parameters)
        return result.result_rows
