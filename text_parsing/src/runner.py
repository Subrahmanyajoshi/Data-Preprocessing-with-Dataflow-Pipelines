import string
import argparse
from argparse import Namespace
from datetime import datetime

import apache_beam as beam

import nltk
from nltk.corpus import stopwords


class Preprocessor(object):
    STOP_WORDS = None # This will be updated by main function at the beginning of execution.

    @staticmethod
    def strip_lines(line: str):
        line = line.strip()
        line = ' '.join(line.split())
        return line

    @staticmethod
    def contract_lines(line: str):
        contraction_dict = {"ain't": "are not", "'s": " is", "aren't": "are not", "don't": "do not",
                            "didn't": "did not", "won't": "will not",
                            "can't": "cannot"}

        words = line.split()
        for i in range(len(words)):
            if words[i] in contraction_dict:
                words[i] = contraction_dict[words[i]]
        return ' '.join(words)

    @staticmethod
    def to_lower_case(line: str):
        return line.lower()

    @staticmethod
    def remove_punctuations(line: str):
        line = line.translate(str.maketrans('', '', string.punctuation))
        return line

    @staticmethod
    def remove_stopwords(line: str):
        return " ".join([word for word in line.split() if word not in Preprocessor.STOP_WORDS])

    @staticmethod
    def remove_special_chars(line: str):
        import re
        line = re.sub('[-+.^:,]', '', line)
        return line

    @staticmethod
    def remove_emojis(line: str):
        import re

        emoj = re.compile("["
                          u"\U0001F600-\U0001F64F"  # emoticons
                          u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                          u"\U0001F680-\U0001F6FF"  # transport & map symbols
                          u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                          u"\U00002500-\U00002BEF"  # chinese char
                          u"\U00002702-\U000027B0"
                          u"\U00002702-\U000027B0"
                          u"\U000024C2-\U0001F251"
                          u"\U0001f926-\U0001f937"
                          u"\U00010000-\U0010ffff"
                          u"\u2640-\u2642"
                          u"\u2600-\u2B55"
                          u"\u200d"
                          u"\u23cf"
                          u"\u23e9"
                          u"\u231a"
                          u"\ufe0f"  # dingbats
                          u"\u3030"
                          "]+", re.UNICODE)
        return re.sub(emoj, '', line)

    @staticmethod
    def split_data(line: str):
        line = line.split(maxsplit=1)
        if len(line) == 2:
            value = line[1]
            label = 1 if line[0] == 'label1' else 0
            return [value, label]


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
            f'--job_name=text-parsing-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
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
                                                 header='text, label',
                                                 num_shards=1)
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

    nltk.download('stopwords')
    
    # Initialize stop words
    Preprocessor.STOP_WORDS = set(stopwords.words('english'))
    
    runner = DataFlowSubmitter(args=args)
    runner.build_and_run()


if __name__ == '__main__':
    main()
