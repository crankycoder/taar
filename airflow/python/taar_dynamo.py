# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This module replicates the scala script over at

https://github.com/mozilla/telemetry-batch-view/blob/1c544f65ad2852703883fe31a9fba38c39e75698/src/main/scala/com/mozilla/telemetry/views/HBaseAddonRecommenderView.scala
"""

import sys
import argparse
from datetime import date
from datetime import timedelta


def parse_args():
    today_str = date.today().strftime("%Y%m%d")
    yesterday_str = (date.today()-timedelta(days=1)).strftime("%Y%m%d")

    desc = 'Copy data from telemetry HBase to DynamoDB'
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('--from',
            dest='from_date',
            action='store',
            default=today_str,
            required=False,
            help='Start date for data submission')

    parser.add_argument('--to',
            dest='to_date',
            action='store',
            default=today_str,
            required=False,
            help='End date of the data submission')

    parser.add_argument('--hbase-master',
            dest='hbase_master',
            action='store',
            required=True,
            help='IP address of HBase master')

    args = parser.parse_args()
    return args


def main(*args):
    """
    This script takes 3 command line arguments:

     --hbase-master $hbase
     --from $date
     --to $date
    """

    args = parse_args()



if __name__ == '__main__':
    main(sys.argv)
