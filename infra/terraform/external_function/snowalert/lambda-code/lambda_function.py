import json
import os.path
import re
import sys
from codecs import encode
from importlib import import_module
from json import dumps, loads
from typing import Any, Dict, Optional, Text
from urllib.parse import urlparse

import boto3

from drivers import destination_s3
from vault import decrypt_if_encrypted

# pip install --target ./site-packages -r requirements.txt
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(dir_path, 'site-packages'))

HTTP_METHOD_STRING = 'httpMethod'
REGION_NAME = 'us-west-2'


def zip(s, chunk_size=1_000_000):
    '''zip in pieces, as it is tough to inflate large chunks in Snowflake per UDF mem limits'''
    do_zip = lambda s: encode(encode(s.encode(), encoding='zlib'), 'base64').decode()
    if len(s) > chunk_size:
        return [do_zip(s[:chunk_size])] + zip(s[chunk_size:], chunk_size)
    return [do_zip(s)]


def format(s, ps):
    """format string s with params ps, preserving type of singular references

    >>> format('{0}', [{'a': 'b'}])
    {'a': 'b'}

    >>> format('{"z": [{0}]}', [{'a': 'b'}])

    """

    def replace_refs(s, ps):
        for i, p in enumerate(ps):
            old = '{' + str(i) + '}'
            new = dumps(p) if isinstance(p, (list, dict)) else str(p)
            s = s.replace(old, new)
        return s

    m = re.match('{(\d+)}', s)
    return ps[int(m.group(1))] if m else replace_refs(s, ps)


def create_response(code, msg):
    return {'statusCode': code, 'body': msg}


def invoke_process_lambda(batch_id, data, lambda_name):
    # Create payload to be sent to lambda
    invoke_payload = json.dumps(data)

    # Invoke processing lambda asynchronously by using InvocationType='Event'.
    # This allows the processing to continue while the POST handler returns HTTP 202.
    lambda_client = boto3.client(
        'lambda',
        region_name=REGION_NAME,
    )
    lambda_response = lambda_client.invoke(
        FunctionName=lambda_name, InvocationType='Event', Payload=invoke_payload
    )
    # returns 202 on success if InvocationType = 'Event'
    return lambda_response


def async_flow_poll(event: Any, destination: str, context: Any) -> Dict[str, Any]:
    """
    Repeatedly checks on the status of the batch_ID, and returns the result after the
    processing has been completed

    Args:
        event (Any):
        destination (str):
        context (Any):

    Returns:
        Dict[str, Any]:
    """
    batch_id = event['headers']['sf-external-function-query-batch-id']
    processed_data = read_data_from_storage(batch_id)
    # print(f'data from storage in poll {processed_data}')
    def parse_processed_data(response):
        # in this case, the response is the response from DynamoDB
        response_metadata = response['ResponseMetadata']
        status_code = response_metadata['HTTPStatusCode']

        # Take action depending on item status
        item = response['Item']
        job_status = item['status']
        if job_status == SUCCESS_STATUS:

            file = item['data']
            s3 = boto3.client('s3')

            obj = s3.get_object(Bucket=IT_DATA_INGEST_BUCKET, Key=file)
            data = eval(obj['Body'].read().decode('utf-8'))

            return {'statusCode': 200, 'body': json.dumps({'data': data})}
        elif job_status == IN_PROGRESS_STATUS:
            return {'statusCode': 202, "body": "{}"}
        else:
            return create_response(500, "Error in poll: Unknown item status.")

    return parse_processed_data(processed_data)


def async_flow_init(event, context):
    """
    - Reads batch_ID and data from the request,
    - marks the batch_ID as being processed, and
    - starts the processing service.
    """
    batch_id = event['headers']['sf-external-function-query-batch-id']
    destination = event['headers']['sf-custom-destination']

    lambda_name = context.function_name
    write_driver = import_module(f'drivers.write_{urlparse(destination).scheme}')
    write_driver.init(destination, batch_id)
    lambda_response = invoke_process_lambda(batch_id, event, lambda_name)

    # lambda response returns 202, because we are invoking it with
    # InvocationType = 'Event'
    if lambda_response["StatusCode"] != 202:
        response = create_response(
            400, "Error in inititate: processing lambda not started"
        )
    else:
        response = {'statusCode': lambda_response["StatusCode"]}

    return response


def lambda_handler(event, context):
    destination = event['headers'].get('sf-custom-destination')
    method = event.get('httpMethod', 'GET')

    if destination:  # This requires async flow
        return async_flow_init(event, context)
    elif method == 'GET':  # This requires polling the write destination
        return async_flow_poll(event, context)
    elif method == 'POST':  # This means it's request from API gateway
        return sync_flow(event, context)


def sync_flow(event, context=None):
    headers = event['headers']
    response_encoding = headers.pop('sf-custom-response-encoding', None)

    req_body = loads(event['body'])
    res_data = []
    for row_number, *args in req_body['data']:
        row_result = []
        process_row_params = {
            k.replace('sf-custom-', '').replace('-', '_'): format(v, args)
            for k, v in headers.items()
            if k.startswith('sf-custom-')
        }

        try:
            driver, *path = event['path'].lstrip('/').split('/')
            driver = driver.replace('-', '_')
            process_row = import_module(f'drivers.process_{driver}').process_row
            row_result = process_row(*path, **process_row_params)

        except Exception as e:
            row_result = {'error': repr(e)}

        res_data.append(
            [
                row_number,
                zip(dumps(row_result)) if response_encoding == 'gzip' else row_result,
            ]
        )
    data_dumps = dumps({'data': res_data})
    if len(data_dumps) > 6_000_000:
        data_dumps = dumps(
            {
                'data': [
                    [
                        rn,
                        {
                            'error': (
                                f'Response size ({len(data_dumps)} bytes) will likely'
                                'exceeded maximum allowed payload size (6291556 bytes).'
                            )
                        },
                    ]
                    for rn, *args in req_body['data']
                ]
            }
        )

    return {'statusCode': 200, 'body': data_dumps}
