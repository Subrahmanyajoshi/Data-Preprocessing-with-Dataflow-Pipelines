# Google-Cloud-Dataflow-Pipelines
A repository to show how to use Google Cloud's Dataflow pipelines for data preprocessing using Apache beam in python 

- In this repo I have used 2 different procedures which use apache beams pipelines and run on Dataflow.
- Jobs can be submitted to Dataflow either through notebook directly or using a python script.
- The same apache beam pipelines can also be run locally by toggling a single parameter of pipeline. 

### Important:
- Notebooks I have mentioned above are Google Cloud vertex ai workbench notebooks run in Google Cloud with 
apache beam environment.
- All the python scripts are also run in vertex ai notebooks environment itself.

### Install Dependencies
```shell
pip install requirements.txt
```

## Csv Reader
- Reads csv data stored in bigquery, groups it on the basis of a field and writes details of each dataframe created 
into google cloud storage output directory path specified.
- Notebook has the capability to read data from csv files and push it to a bigquery table.
- Executing following commands after navigating to csv_reader/src/.
### Running locally
```shell
python3 runner.py --project=<project-id> --bucket=<bucket-name> --bigquery-table=<bigquery-table-id> --output-path=<output-dir> --direct-runner
```
### Running on Dataflow
```shell
python3 runner.py --project=<project-id> --bucket=<bucket-name> --bigquery-table=<bigquery-table-id> --output-path=<output-dir> --dataflow-runner
```

## Text parser
- Reads a huge input text file, applies multiple preprocessing techniques on it.
- Output of this pipeline is a csv file containing cleaned text data and labels.
- Executing following commands after navigating to text_parsing/src/.

### Running locally
```shell
python3 runner.py --project=<project-id> --bucket=<bucket-name> --input-path=<path-to-input>  --output-dir=<output-dir> --direct-runner
```

### Running on Dataflow
```shell
python3 runner.py --project=<project-id> --bucket=<bucket-name> --input-path=<path-to-input>  --output-dir=<output-dir> --dataflow-runner
```
