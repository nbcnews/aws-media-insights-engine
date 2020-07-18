import boto3
import os
import logging
import sys
import requests
import json

MIE_POOL_ID = str(os.environ['UserPoolId'])
MIE_CLIENT_ID = str(os.environ['PoolClientId'])
MIE_USER_NAME = str(os.environ['UserName'])
MIE_USER_PWD = str(os.environ['UserPwd'])
MIE_WORKFLOW_ENDPOINT = str(os.environ['WorkflowEndpoint'])


def lambda_handler(event, context):
    s3 = event['Records'][0]['s3']
    config = workflow_config(s3['bucket']['name'], s3['object']['key'])
    try:
        token = authenticate_and_get_token(MIE_USER_NAME, MIE_USER_PWD, MIE_POOL_ID, MIE_CLIENT_ID)
        run_workflow(config, token)
    except Exception as e:
        logging.error(f'failed to process message {s3}: {e}')
        raise e


def run_workflow(config: str, token: str):
    try:
        resp = requests.post(
            url=MIE_WORKFLOW_ENDPOINT + 'workflow/execution',
            data=config,
            headers={
                'Content-Type':'application/json',
                'Authorization':token
            }
        )
        resp.raise_for_status()
    except Exception as e:
        raise e

def authenticate_and_get_token(username: str, password: str, 
                               pool_id: str, client_id: str) -> str:
    client = boto3.client('cognito-idp')

    resp = client.admin_initiate_auth(
        UserPoolId=pool_id,
        ClientId=client_id,
        AuthFlow='ADMIN_NO_SRP_AUTH',
        AuthParameters={
            'USERNAME': username,
            'PASSWORD': password
        }
    )
    return resp['AuthenticationResult']['IdToken']

def workflow_config(bucket: str, key: str) -> dict:
    config =  {
            'Name': 'MieCompleteWorkflow',
            'Configuration': {
                'defaultPrelimVideoStage': {
                    'Thumbnail': {
                        'ThumbnailPosition': '10',
                        'Enabled': True
                    },
                    'Mediainfo': {
                        'Enabled': True
                    }
                },
                'defaultVideoStage': {
                    'faceDetection': {
                        'Enabled': True
                    },
                    'technicalCueDetection': {
                        'Enabled': True
                    },
                    'shotDetection': {
                        'Enabled': True
                    },
                    'celebrityRecognition': {
                        'Enabled': True
                    },
                    'labelDetection': {
                        'Enabled': True
                    },
                    'Mediaconvert': {
                        'Enabled': True
                    },
                    'contentModeration': {
                        'Enabled': True
                    },
                    'faceSearch': {
                        'Enabled': False,
                        'CollectionId': 'undefined'
                    },
                    'textDetection': {
                        'Enabled': True
                    },
                    'GenericDataLookup': {
                        'Enabled': False,
                        'Bucket': 'mie-dataplane-1oufs3l5cabvb',
                        'Key': 'undefined'
                    }
                    },
                    'defaultAudioStage': {
                        'Transcribe': {
                            'Enabled': True,
                            'TranscribeLanguage': 'en-US'
                        }
                    },
                    'defaultTextStage': {
                        'Translate': {
                            'Enabled': False,
                            'SourceLanguageCode': 'en',
                            'TargetLanguageCode': 'es'
                        },
                        'ComprehendEntities': {
                            'Enabled': True
                        },
                        'ComprehendKeyPhrases': {
                            'Enabled': True
                        }
                    },
                    'defaultTextSynthesisStage': {
                        'Polly': {
                            'Enabled': False
                        }
                }
            },
            'Input': {
                'Media': {
                    'Video': {
                        'S3Bucket': '{}'.format(bucket),
                        'S3Key': '{}'.format(key)
                    }
                }
            }
        }
    return json.dumps(config)