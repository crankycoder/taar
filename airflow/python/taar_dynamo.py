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

from pyspark.sql import functions as F
from pyspark.sql import Window


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

def etl(from_date, to_date, dataFrame):
    errAcc = spark.sparkContext.accumulator("Validation Errors")

    for offset in range(0, to_date-from_date):
        currentDate = from_date + offset
        currentDateString = currentDate.strformat("%Y%m%d")
        print("Processing %s" % currentDateString)


        # Get the data for the desired date out of the dataframe
        data = dataFrame(currentDateString)

        # Get the most recent (client_id, subsession_start_date) tuple
        # for each client since the main_summary might contain
        # multiple rows per client. We will use it to filter out the
        # full table with all the columns we require.

        # TODO: verify the '$' notation for pyspark vs scala spark
        clientShortList = data.select("client_id",
                "subsession_start_date",
                F.row_number()
                    .over(Window.partitionBy("client_id")
                        .orderBy(F.desc("subsession_start_date")))
                    .alias("clientid_rank"))
                .where("clientid_rank" == 1)
                .drop("clientid_rank")

        dataSubset = data.select(
                "client_id",
                "subsession_start_date",
                "subsession_length",
                "city",
                "locale",
                "os",
                "places_bookmarks_count",
                "scalar_parent_browser_engagement_tab_open_event_count",
                "scalar_parent_browser_engagement_total_uri_count",
                "scalar_parent_browser_engagement_unique_domains_count",
                "active_addons",
                "disabled_addons_ids")

         # Join the two tables: only the elements in both dataframes
         # will make it through.

         # TODO: Check that scala.collection.Seq is safely ported to
         # vanilla Python lists
         clientsData = dataSubset.join(
                 clientsShortList, ["client_id", 'subsession_start_date'])


          # Convert the DataFrame to JSON and get an RDD out of it.
          subset = clientsData.select("client_id", "subsession_start_date")

          # TODO: This looks wrong.  Try this in the interpreter
          jsonData = clientsData.select("city",
                                        "subsession_start_date",
                                        "subsession_length",
                                        "locale",
                                        "os",
                                        "places_bookmarks_count",
                                        "scalar_parent_browser_engagement_tab_open_event_count",
                                        "scalar_parent_browser_engagement_total_uri_count",
                                        "scalar_parent_browser_engagement_unique_domains_count",
                                        "active_addons",
                                        "disabled_addons_ids").toJSON().rdd

          # Build an RDD containing (key, document) tuples: one per client.
          rdd = subset.rdd.zip(jsonData).flatMap{ case (row, json) =>
                  clientId = row.getString(0)
                  startDate = row.getString(1)

                  try {
                      assert(clientId != null && startDate != null)
                      // Validate the subsession start date: parsing will throw if
                      // the date format is not valid.
                      val dateFormatter = org.joda.time.format.ISODateTimeFormat.dateTime()
                      dateFormatter.parseDateTime(startDate)
                      Some((clientId, Seq(json)))
                      } catch {
                          case _: Throwable =>
                          errorAcc.add(1)
                          None
                          }
                      }

        // Bulk-send the RDD to HBase.
        rdd.toHBaseBulk(tableName, columnFamily, List(column))
    println(s"${errorAcc.value} validation errors encountered")





def main(*args):
    """
    This script takes 3 command line arguments:

     --hbase-master $hbase
     --from $date
     --to $date
    """

    args = parse_args()
    etl(args.from_date, 
        args.to_date, 
        spark.read.parquet("s3://telemetry-parquet/main_summary/%(MainSummaryView.schemaVersion)s/submission_date_s3="))



if __name__ == '__main__':
    main(sys.argv)
