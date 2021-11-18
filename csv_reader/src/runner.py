import argparse
from argparse import Namespace
from datetime import datetime

import apache_beam as beam
import pandas as pd


class DataFlowSubmitter(object):

    def __init__(self, args: Namespace):
        self.project = args.project
        self.bucket = args.bucket
        self.runner = 'DirectRunner'
        self.bigquery_table = args.bigquery_table
        self.output_path = args.output_path

        if args.direct_runner and args.dataflow_runner:
            raise ValueError('Please specify only one of the options. either direct runner or dataflow runner')

        if args.dataflow_runner:
            self.runner = 'DataFlowRunner'

    @staticmethod
    def convert_namespace(element):
        return Namespace(**element)

    @staticmethod
    def convert_to_df(element):
        ns_list = element[1]
        ns_list = [vars(ele) for ele in ns_list]
        df = pd.DataFrame(ns_list)
        return df

    @staticmethod
    def get_info(df):
        return {'columns': list(df.columns), 'Shape': df.shape}

    def build_and_run(self):

        argv = [
            f'--project={self.project}',
            f'--job_name=csv_runner-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
            '--save_main_session',
            f'--staging_location=gs://{self.bucket}/staging/',
            f'--temp_location=gs://{self.bucket}/temp/',
            '--region=us-central1',
            f'--runner={self.runner}'
        ]

        p = beam.Pipeline(argv=argv)

        (p
         | 'Get data from BigQuery' >> beam.io.ReadFromBigQuery(query=f'Select * From {self.bigquery_table}',
                                                                use_standard_sql=True,
                                                                project='text-analysis-323506')
         | 'Convert to namespace' >> beam.Map(DataFlowSubmitter.convert_namespace)
         | 'Groupby subject' >> beam.GroupBy('Subject')
         | 'Convert to dataframe' >> beam.Map(DataFlowSubmitter.convert_to_df)
         | 'Get dataset info' >> beam.Map(DataFlowSubmitter.get_info)
         | 'Write info to Cloud Storage' >> beam.io.WriteToText(f"gs://{self.bucket}/output/csv_details-output.txt")
         )
        p.run()


def main():
    parser = argparse.ArgumentParser(description='Running Apache Beam pipelines on Dataflow')
    parser.add_argument('--project', type=str, required=True, help='Project id')
    parser.add_argument('--bucket', type=str, required=True, help='Name of the bucket to host dataflow components')
    parser.add_argument('--bigquery-table', type=str, required=True, help='id of bigquery table')
    parser.add_argument('--output-path', type=str, required=True, help='output path to store results')
    parser.add_argument('--direct-runner', type=str, required=False, action='store_true')
    parser.add_argument('--dataflow-runner', type=str, required=False, action='store_true')
    args = parser.parse_args()

    runner = DataFlowSubmitter(args=args)
    runner.build_and_run()


if __name__ == '__main__':
    main()
