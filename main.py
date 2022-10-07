from google.cloud import storage
from flask import make_response
import functions_framework
import os
import uuid
import plotly.graph_objects as go
import plotly.io as pio
import pandas as pd
import tempfile


def get_bucket_name() -> str:
    return os.environ.get('CLOUD_STORAGE_BUCKET_NAME', 'my-bucket-id')


def upload_blob_and_return_url(chart_local_file_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(get_bucket_name())
    blob = bucket.blob(chart_local_file_name)

    blob.upload_from_filename(tempfile.gettempdir(
    ) + "/"+chart_local_file_name, content_type='image/png')
    return blob.public_url


def create_chart_name() -> str:
    return "chart_{}.png".format(str(uuid.uuid4()))


def convert_dataframe_to_chart(df: pd.DataFrame, has_market_hours=False) -> str:
    fig = go.Figure(data=[go.Candlestick(x=df['timestamp'],
                                         open=df['Open'], high=df['High'],
                                         low=df['Low'], close=df['Close'])
                          ])

    fig.update_layout(xaxis_rangeslider_visible=False)
    fig.update_layout(template=pio.templates['plotly_dark'])

    if has_market_hours:
        fig.update_xaxes(rangebreaks=[dict(bounds=[0, 9], pattern="hour"),
                                      dict(bounds=['sat', 'mon'])])

    chart_name = create_chart_name()
    fig.write_image(tempfile.gettempdir() + "/"+chart_name,
                    format='png', width=1200, height=800)
    return chart_name


@functions_framework.http
def get_crypto_ticker_graph(request):

    request_json = request.get_json(silent=True)
    if request_json is None:
        return make_response("Bad request JSON", 400)

    df = pd.DataFrame(request_json, columns=[
                      'timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    chart_name = convert_dataframe_to_chart(df)
    chart_url = upload_blob_and_return_url(chart_name)
    return chart_url


@functions_framework.http
def get_stock_ticker_graph(request):

    request_json = request.get_json(silent=True)
    if request_json is None:
        return make_response("Bad request JSON", 400)

    transformed_array = []
    for x in range(len(request_json['c'])):
        transformed_array.append([request_json['t'][x], request_json['o'][x], request_json['h']
                                 [x], request_json['l'][x], request_json['c'][x], request_json['v'][x]])
    df = pd.DataFrame(transformed_array, columns=[
                      'timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    chart_name = convert_dataframe_to_chart(df, has_market_hours=True)
    chart_url = upload_blob_and_return_url(chart_name)
    return chart_url
