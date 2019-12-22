import singer

from google.cloud import bigquery
from google.oauth2 import service_account

LOGGER = singer.get_logger()
SCOPES = ['https://www.googleapis.com/auth/bigquery','https://www.googleapis.com/auth/bigquery.insertdata']

class BigQuery():
    def __init__(self, config):
        self.service_account_file = config['service_account_file']
        self.project_id = config['project_id']
        self.dateset_id = config['dataset_id']
        self.validate_records = config['validate_records']
        self.full_replication = config['full_replication']
        self.credentials = self.init_credentials(config)
        self.client = self.init_client()

    def init_credentials(self, config):
        return service_account.Credentials.from_service_account_file(
            config['service_account_file'],
            scopes=SCOPES,
        )

    def init_client(self):
        return bigquery.Client(
            credentials=self.credentials,
            project=self.project_id
        )

    def persist_lines(self, lines=None):
        state = None
        schemas = {}
        key_properties = {}
        tables = {}
        rows = {}
        errors = {}

        for line in lines:
            try:
                msg = singer.parse_message(line)
            except json.decoder.JSONDecodeError:
                LOGGER.error("Unable to parse:\n{}".format(line))
                raise

            if isinstance(msg, singer.RecordMessage):
                if msg.stream not in schemas:
                    raise Exception("A record for stream {} was encountered before a corresponding schema".format(msg.stream))

                schema = schemas[msg.stream]

                if validate_records:
                    validate(msg.record, schema)

                # NEWLINE_DELIMITED_JSON expects literal JSON formatted data, with a newline character splitting each row.
                dat = bytes(json.dumps(msg.record) + '\n', 'UTF-8')

                rows[msg.stream].write(dat)
                #rows[msg.stream].write(bytes(str(msg.record) + '\n', 'UTF-8'))

                state = None

            elif isinstance(msg, singer.StateMessage):
                LOGGER.debug('Setting state to {}'.format(msg.value))
                state = msg.value

            elif isinstance(msg, singer.SchemaMessage):
                table = msg.stream
                schemas[table] = msg.schema
                key_properties[table] = msg.key_properties
                #tables[table] = bigquery.Table(dataset.table(table), schema=build_schema(schemas[table]))
                rows[table] = TemporaryFile(mode='w+b')
                errors[table] = None
                # try:
                #     tables[table] = bigquery_client.create_table(tables[table])
                # except exceptions.Conflict:
                #     pass

            else:
                raise Exception("Unrecognized message {}".format(msg))

        for table in rows.keys():
            table_ref = self.client.dataset(dataset_id).table(table)
            SCHEMA = build_schema(schemas[table])
            load_config = LoadJobConfig()
            load_config.schema = SCHEMA
            load_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON

            if truncate:
                load_config.write_disposition = WriteDisposition.WRITE_TRUNCATE

            rows[table].seek(0)
            logger.info("loading {} to Bigquery.\n".format(table))
            load_job = self.client.load_table_from_file(
                rows[table], table_ref, job_config=load_config)
            logger.info("loading job {}".format(load_job.job_id))
            logger.info(load_job.result())

