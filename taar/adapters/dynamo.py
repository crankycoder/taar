import boto3
import datetime


class ProfileController(object):
    """
    This class
    """

    def __init__(self, local_instance=False):
        """
        Configure access to the DynamoDB isntance
        """
        if local_instance:
            self._ddb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
        else:
            self._ddb = boto3.resource('dynamodb')

        self._table = self._ddb.Table('taar_addon_data')

    def list_tables(self):
        return list(self._ddb.tables.all())

    def scan_table(self):
        """Dump the entire dynamoDB table
        """
        response = self._table.scan()
        items = response['Items']
        return items

    def get_client_profile(self, client_id):
        """This fetches a single client record out of DynamoDB
        """
        response = self._table.get_item(Key={'client_id': client_id})
        return response['Item']

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


if __name__ == '__main__':
    pc = ProfileController(local_instance=True)
    print(pc.list_tables())

    blobs = []
    for i in range(100):
        client_id = "%d_%s" % (i, datetime.datetime.now().isoformat())
        blob = {"client_id": client_id,
                "bookmark_count": 5,
                "disabled_addon_ids": ["disabled1", "disabled2"],
                "geo_city": "Toronto",
                "installed_addons": ["active1", "active2"],
                "locale": "en-CA",
                "os": "Mac OSX",
                "profile_age_in_weeks": 5,
                "profile_date": "2018-Jan-08",
                "submission_age_in_weeks": 5,
                "submission_date": "2018-Jan-09",
                "subsession_length": 20,
                "tab_open_count": 10,
                "total_uri": 9,
                "unique_tlds": 5}
        blobs.append(blob)

    pc.batch_put_clients(blobs)
    print("------- Table Scan and delete below ----------")
    for i, rec in enumerate(pc.scan_table()):
        print("Deleting: %s" % rec['client_id'])
        pc.delete(rec['client_id'])
    print("Deleted %d records" % (i + 1))
