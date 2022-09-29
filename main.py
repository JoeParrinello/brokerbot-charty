from google.cloud import storage
from flask import make_response
import functions_framework
import os
import uuid
import plotly.graph_objects as go
import pandas as pd
import tempfile

def get_bucket_name() -> str:
    return os.environ.get('CLOUD_STORAGE_BUCKET_NAME', 'my-bucket-id')

def upload_blob_and_return_url(chart_local_file_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(get_bucket_name())
    blob = bucket.blob(chart_local_file_name)

    blob.upload_from_filename(tempfile.gettempdir() +"/"+chart_local_file_name, content_type='image/png')
    return blob.public_url

def create_chart_name() -> str:
    return "chart_{}.png".format(str(uuid.uuid4()));

def convert_dataframe_to_chart(df: pd.DataFrame) -> str:
    fig = go.Figure(data=[go.Candlestick(x=df['Datetime'],
                open=df['Open'], high=df['High'],
                low=df['Low'], close=df['Close'])
                     ])

    fig.update_layout(xaxis_rangeslider_visible=False)

    chart_name = create_chart_name()
    fig.write_image(tempfile.gettempdir() +"/"+chart_name, format='png', width=300, height=200)
    return chart_name

@functions_framework.http
def get_ticker_graph(request):

  request_json = request.get_json(silent=True)
  if request_json is None:
    return make_response("Bad request JSON", 400)

  df = pd.DataFrame(request_json, columns=['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume'])
  df['Datetime']= pd.to_datetime(df['Datetime'])
  chart_name = convert_dataframe_to_chart(df)
  chart_url = upload_blob_and_return_url(chart_name)

  return chart_url