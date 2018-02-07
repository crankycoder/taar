import boto3
import json


class ProfileController(object):
    """
    This class
    """

    def __init__(self, region_name, table_name):
        """
        Configure access to the DynamoDB isntance
        """
        self._ddb = boto3.resource('dynamodb', region_name=region_name)
        self._table = self._ddb.Table(table_name)

    def list_tables(self):
        return list(self._ddb.tables.all())

    def _scan_table(self):
        """Dump the entire dynamoDB table.  This is almost certainly a
        bad thing to do.
        """
        response = self._table.scan()
        items = response['Items']
        return items

    def get_client_profile(self, client_id):
        """This fetches a single client record out of DynamoDB
        """
        response = self._table.get_item(Key={'client_id': client_id})
        return json.loads(response['Item']['json_payload'])

    def put_client_profile(self, json_blob):
        """Store a single data record
        """
        response = self._table.put_item(Item=json_blob)
        return response

    def delete(self, client_id):
        self._table.delete_item(Key={'client_id': client_id})

    def batch_delete(self, *client_ids):
        with self._table.batch_writer() as batch:
            for client_id in client_ids:
                batch.delete_item(Key={'client_id': client_id})

    def batch_put_clients(self, records):
        """Batch fill the DynamoDB instance with
        """
        with self._table.batch_writer() as batch:
            for rec in records:
                batch.put_item(Item=rec)
