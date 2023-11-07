import spacy
import pandas as pd
import argparse
from datetime import datetime
import boto3
import io
from pandas import DataFrame
import pyarrow as pa
import pyarrow.parquet as pq
MODEL_PATH = './model-best'
# FILE_PATH = './inputs/simply_hired_test101.csv'

s3_client = boto3.client('s3', aws_access_key_id='your_access_key', aws_secret_access_key='your_aws_secret_key')


nlp = spacy.load(MODEL_PATH)


def extract_entities_inlist(job_description):
    """Extracts 14 types of entities from the given
    job description and returns dictionary of
    labels and corresponding list of entities.
    """
    
    try:
        doc = nlp(job_description)
        keys = nlp.get_pipe("ner").labels
        return {
            key: [entity.text for entity in doc.ents 
                if entity.label_ == key]
                for key in keys
            }
    except Exception as e:
        pass
        print("Error : ", e)

def write_dataframe_to_s3(s3_client, bucket_name: str, date_dict: dict, df: DataFrame) -> None:
        try:
            
            table = pa.Table.from_pandas(df)

            # Create an in-memory buffer to store the Parquet data
            buffer = io.BytesIO()

            # Write the Parquet Table to the buffer
            pq.write_table(table, buffer)

            # Upload the buffer to S3
            response = s3_client.put_object(
                Bucket=bucket_name,
                Key=f"simply_hired_extracted/clean_simply_hired_with_dept/year={date_dict['year']}/month={date_dict['month']}/day={date_dict['day']}/clean_simply_hired_with_dept.parquet",
                Body=buffer.getvalue()
            )
            
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
        except Exception as e:
            print(f'Exception: {e}')       

def generate_output_csv(filepath):
    # filename = filepath.removesuffix('.csv')
    filename = filepath.rsplit('/', 1)[1][:-4]
    data_df = pd.read_csv(filepath)
    data_df['entity_info'] = data_df['job_description'].apply(extract_entities_inlist)
    df_normalized = pd.json_normalize(data_df['entity_info'])
    df_concatenated = pd.concat([data_df, df_normalized], axis=1)
    df_concatenated = df_concatenated.drop('entity_info', axis=1)
    df_concatenated.to_csv(f'./outputs/{filename}_out.csv')
    # df_concatenated.to_parquet(f'./outputs/{filename}_out.parquet')
    

    date_today = str(datetime.now().date())
    date_dict = {
        'year': date_today.split('-')[0],
        'month': date_today.split('-')[1],
        'day': date_today.split('-')[2]
    }

    write_dataframe_to_s3(
    s3_client=s3_client,
    bucket_name='fuse-internal-analytics-jobs-scraping-dev',
    date_dict=date_dict,
    df=df_concatenated
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-fp',
                        '--filepath',
                        help='Enter the path to the csv file',
                        required='True')
    args = parser.parse_args()

    FILE_PATH = args.filepath
    generate_output_csv(FILE_PATH)


