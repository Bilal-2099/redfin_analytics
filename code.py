from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import boto3

# S3 client and target bucket
s3_client = boto3.client('s3')
target_bucket_name = 'redfin-transforming-zone-yml'

# Data source URL
url_by_city = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz'


def extract_data(**kwargs):
    """
    Extract data from the source URL and save it as a CSV file locally.
    """
    url = kwargs['url']
    df = pd.read_csv(url, compression='gzip', sep='\t')

    # Generate unique file name
    now = datetime.now().strftime("%d%m%Y%H%M%S")
    file_name = f"redfin_data_{now}.csv"
    local_file_path = f"/home/ubuntu/{file_name}"

    # Save CSV locally
    df.to_csv(local_file_path, index=False)

    return {"local_path": local_file_path, "file_name": file_name}


def transform_data(task_instance):
    """
    Transform the extracted data and upload it to the S3 bucket.
    """
    xcom_data = task_instance.xcom_pull(task_ids="extract_redfin_data")
    local_path = xcom_data["local_path"]
    file_name = xcom_data["file_name"]

    # Load data
    df = pd.read_csv(local_path)

    # Transformations: Remove commas in 'city', filter columns, and manipulate dates
    df['city'] = df['city'].str.replace(',', '', regex=False)
    cols = [
        'period_begin', 'period_end', 'region_type', 'city', 'state',
        'median_sale_price', 'median_list_price', 'homes_sold', 'inventory'
    ]
    df = df[cols].dropna()

    # Add derived columns for year and month
    df['period_begin'] = pd.to_datetime(df['period_begin'])
    df['period_end'] = pd.to_datetime(df['period_end'])
    df['period_begin_year'] = df['period_begin'].dt.year
    df['period_end_year'] = df['period_end'].dt.year
    df['period_begin_month'] = df['period_begin'].dt.strftime('%b')
    df['period_end_month'] = df['period_end'].dt.strftime('%b')

    # Convert DataFrame to CSV and upload to S3
    csv_data = df.to_csv(index=False)
    s3_client.put_object(
        Bucket=target_bucket_name,
        Key=f"transformed/{file_name}",
        Body=csv_data
    )


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
    'start_date': datetime(2024, 9, 29)
}

# Define DAG
with DAG(
    'redfin_analytics_dag',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False
) as dag:

    # Task to extract data
    extract_redfin_data = PythonOperator(
        task_id='extract_redfin_data',
        python_callable=extract_data,
        op_kwargs={'url': url_by_city}
    )

    # Task to transform data
    transform_redfin_data = PythonOperator(
        task_id='transform_redfin_data',
        python_callable=transform_data
    )

    # Task to upload raw data to S3
    load_to_s3 = BashOperator(
        task_id='load_to_s3',
        bash_command=(
            'aws s3 mv {{ ti.xcom_pull("extract_redfin_data")["local_path"] }} '
            's3://storing-the-raw-data-yml/raw/'
        )
    )

    # Task dependencies
    extract_redfin_data >> transform_redfin_data >> load_to_s3