import ndjson
from google.cloud import bigquery
import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

class BigQueryTools():

    def __init__(self, configfile: str):
        """
        Constructor
        :param configuration:  YAML file related to the connection and the table description.
        """

        # Read the config
        with open(configfile) as cfg:
            stream = cfg.read()

        configuration = yaml.load(stream, Loader=Loader)

        # Set config
        self.auth_file = configuration['auth']['file']
        self._dataset = configuration['destination']['datasetid']
        self._table = configuration['destination']['tableid']
        self._schema = configuration['load']['schema']

        # File config
        self._path = configuration['source']['local']['dir']
        self._filename = configuration['source']['local']['filename']

        # Set connection
        try:
            self._client = bigquery.Client.from_service_account_json(self.auth_file)
        except TypeError:
            raise FileExistsError

        self._dataset_ref = self._client.dataset(self._dataset)


    def to_ndjson(self, json_data: list, mode='w', log=True):
        """
        Transform a json object into a ND Json
        :param json_data: Data to being loaded
        """
        with open(self._path + self._filename, mode) as f:
            ndjson.dump(json_data, f)
            f.write('\n')
            if log is True:
                print('File {} created.'.format(self._filename))

    def upload_from_ndjson(self):
        """
        Upload the data to BigQuery
        :param json_data: Data to being loaded
        """

        #self.to_ndjson(json_data, self._path, self._filename)
        #Set config
        job_config = bigquery.LoadJobConfig()
        #job_config.schema = self._schema
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        """job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='date',  # name of column to use for partitioning
            expiration_ms=7776000000)  # 90 days"""

        # Open the source file that contains the data to being loaded
        with open(self._path + self._filename, 'rb') as source_file:
            load_job = self._client.load_table_from_file(
                source_file,
                self._dataset_ref.table(self._table),
                job_config=job_config,
            )  # API request
            print("Starting job {}".format(load_job.job_id))

            try:
                load_job.result()  # Waits for table load to complete.
                print("Job finished.")
                destination_table = self._client.get_table(self._dataset_ref.table(self._table))
                print("Loaded {} rows.".format(destination_table.num_rows))
            except:
                print("Error: {}".format(load_job.errors))

    def streaming_data_into_a_table(self, json_data: list):
        """
        Streaming data into a table. The table must already exist and have a defined schema
        :param json_data: Data to stream into BigQuery. The format must like be a schema
        """
        table_ref = self._client.dataset(self._dataset).table(self._table)
        table = self._client.get_table(table_ref)

        errors = self._client.insert_rows_json(table, json_data)

        assert errors == [], errors
