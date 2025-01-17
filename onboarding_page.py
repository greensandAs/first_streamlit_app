import streamlit as st
import teradatasql
import pandas as pd
import boto3
from io import StringIO

# Streamlit UI for Teradata Connection
st.title("Teradata to AWS S3 Exporter")

st.sidebar.header("Teradata Connection Details")
teradata_host = st.sidebar.text_input("Host")
teradata_user = st.sidebar.text_input("User")
teradata_password = st.sidebar.text_input("Password", type="password")
database_name = st.sidebar.text_input("Database Name")
table_name = st.sidebar.text_input("Table Name")

# AWS S3 Connection Details
st.sidebar.header("AWS S3 Details")
aws_access_key_id = st.sidebar.text_input("AWS Access Key ID")
aws_secret_access_key = st.sidebar.text_input("AWS Secret Access Key", type="password")
bucket_name = st.sidebar.text_input("S3 Bucket Name")
s3_file_path = st.sidebar.text_input("S3 File Path (e.g., folder/filename.csv)")

if st.sidebar.button("Export Data"):
    # Connect to Teradata
    conn = teradatasql.connect(host=teradata_host, user=teradata_user, password=teradata_password)
    query = f"SELECT * FROM {database_name}.{table_name}"
    df = pd.read_sql(query, conn)
    conn.close()

    # Convert DataFrame to CSV
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload CSV to S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_path, Body=csv_buffer.getvalue())

    st.success(f"Data from {database_name}.{table_name} successfully exported to {s3_file_path} in S3 bucket {bucket_name}")

