import json
import re
import time
from typing import Any, Dict, Optional, Text

import boto3


def pick(path: str, d: dict):
    # path e.g. "a.b.c"
    retval: Optional[Any] = d
    for p in path.split('.'):
        if p and retval:
            retval = retval.get(p)
    return retval


# from https://requests.readthedocs.io/en/master/_modules/requests/utils/
def parse_header_links(value):
    """Return a list of parsed link headers proxies.

    i.e. Link: <http:/.../front.jpeg>; rel=front; type="image/jpeg",<http://.../back.jpeg>; rel=back;type="image/jpeg"

    :rtype: list
    """
    links = []
    replace_chars = ' \'"'

    value = value.strip(replace_chars)
    if not value:
        return links

    for val in re.split(', *<', value):
        try:
            url, params = val.split(';', 1)
        except ValueError:
            url, params = val, ''

        link = {'url': url.strip('<> \'"')}

        for param in params.split(';'):
            try:
                key, value = param.split('=')
            except ValueError:
                break

            link[key.strip(replace_chars)] = value.strip(replace_chars)

        links.append(link)

    return links


def initiate(event: Dict, context):
    """
    Reads batch_ID and data from the request, marks the batch_ID as being processed, and
    starts the processing service.

    Args:
        event ([type]): [description]
        context ([type]): [description]

    Returns:
        [type]: [description]
    """
    batch_id = event[HEADERS_STRING][BATCH_ID_STRING]
    data = event

    lambda_name = context.function_name

    write_to_storage(batch_id, IN_PROGRESS_STATUS, "NULL")
    lambda_response = invoke_process_lambda(batch_id, data, lambda_name)

    # lambda response returns 202, because we are invoking it with
    # InvocationType = 'Event'
    if lambda_response["StatusCode"] != 202:
        response = create_response(
            400, "Error in inititate: processing lambda not started"
        )
    else:
        response = {'statusCode': lambda_response["StatusCode"]}
    return response


def create_response(code, msg):
    return {'statusCode': code, 'body': msg}


def invoke_process_lambda(batch_id, data, lambda_name):
    # Create payload to be sent to processing lambda
    invoke_payload = json.dumps(data).replace(
        '"sf-custom-call-type": "async"', '"sf-custom-call-type": "sync-parent"'
    )

    # Invoke processing lambda asynchronously by using InvocationType='Event'.
    # This allows the processing to continue while the POST handler returns HTTP 202.
    lambda_client = boto3.client(
        'lambda',
        region_name=REGION_NAME,
    )
    print('before async')

    print(f'invoke payload: {invoke_payload}')
    lambda_response = lambda_client.invoke(
        FunctionName=lambda_name, InvocationType='Event', Payload=invoke_payload
    )
    print(f'after async in')
    # returns 202 on success if InvocationType = 'Event'
    return lambda_response


def write_to_storage(batch_id, status, data):
    # we assume that the table has already been created
    client = boto3.resource('dynamodb')
    table = client.Table(TABLE_NAME)

    s3_client = boto3.client('s3')
    file = 'async_response_' + batch_id + '.json'
    key = 'it/async_system_response/' + file
    response = s3_client.put_object(
        Bucket=IT_DATA_INGEST_BUCKET, Body=str(data), Key=key
    )

    item_ts = int(time.time())
    item_expire_ts = int(time.time() + 60 * 60)

    # Put in progress item in table
    item_to_store = {
        'batch_id': batch_id,
        'status': status,
        'data': key,
        'timestamp': item_ts,
        'ttl': item_expire_ts,
    }
    db_response = table.put_item(Item=item_to_store)


def read_data_from_storage(batch_id):
    # we assume that the table has already been created
    client = boto3.resource('dynamodb')
    table = client.Table(TABLE_NAME)

    response = table.get_item(Key={'batch_id': batch_id}, ConsistentRead=True)
    return response
