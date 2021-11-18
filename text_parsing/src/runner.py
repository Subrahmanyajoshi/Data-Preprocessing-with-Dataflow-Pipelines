import argparse
from argparse import Namespace
from datetime import datetime

import apache_beam as beam

from text_parsing.src.preprocessor import Preprocessor


class DataFlowSubmitter(object):

    def __init__(self, args: Namespace):
        self.project = args.project
        self.bucket = args.bucket
        self.input_path = args.input_path
        self.output_dir = args.output_dir

        if args.direct_runner and args.dataflow_runner:
            raise ValueError('Please specify only one of the options. either direct runner or dataflow runner')

        self.runner = 'DirectRunner'
        if args.dataflow_runner:
            self.runner = 'DataFlowRunner'

    @staticmethod
    def to_csv(line):
        return ','.join([str(entry) for entry in line])

    def build_and_run(self):

        argv = [
            f'--project={self.project}',
            f'--job_name=test-processing-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
            '--save_main_session',
            f'--staging_location=gs://{self.bucket}/text_parsing/staging/',
            f'--temp_location=gs://{self.bucket}/text_parsing/temp/',
            '--region=us-central1',
            f'--runner={self.runner}'
        ]

        p = beam.Pipeline(argv=argv)

        (p
         | 'Read text file' >> beam.io.ReadFromText(self.input_path)
         | 'Strip lines' >> beam.Map(Preprocessor.strip_lines)
         | 'Contract lines' >> beam.Map(Preprocessor.contract_lines)
         | 'Lower case' >> beam.Map(Preprocessor.to_lower_case)
         | 'Remove punctuations' >> beam.Map(Preprocessor.remove_punctuations)
         | 'Remove stopwords' >> beam.Map(Preprocessor.remove_stopwords)
         | 'Remove special characters' >> beam.Map(Preprocessor.remove_special_chars)
         | 'Remove emojis' >> beam.Map(Preprocessor.remove_emojis)
         | 'Split data' >> beam.Map(Preprocessor.split_data)
         | 'Filter none values' >> beam.Filter(lambda x: x is not None)
         | 'To csv' >> beam.Map(DataFlowSubmitter.to_csv)
         | 'Write as csv' >> beam.io.WriteToText(f'{self.output_dir}/train',
                                                 file_name_suffix='.csv',
                                                 header='text, label')
         )

        p.run()


def main():
    parser = argparse.ArgumentParser(description='Running Apache Beam pipelines on Dataflow')
    parser.add_argument('--project', type=str, required=True, help='Project id')
    parser.add_argument('--bucket', type=str, required=True, help='Name of the bucket to host dataflow components')
    parser.add_argument('--input-path', type=str, required=True, help='path to input data')
    parser.add_argument('--output-dir', type=str, required=True, help='output dir path to store results')
    parser.add_argument('--direct-runner', required=False, action='store_true')
    parser.add_argument('--dataflow-runner', required=False, action='store_true')
    args = parser.parse_args()

    runner = DataFlowSubmitter(args=args)
    runner.build_and_run()


if __name__ == '__main__':
    main()
