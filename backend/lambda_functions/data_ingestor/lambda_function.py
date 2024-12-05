import json
import boto3
import requests
from datetime import datetime
import uuid
import logging
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('maritime-transit-data')


def fetch_maritime_data():
    """Fetch data from maritime transit APIs"""
    apis = [
        {
            'name': 'OpenSkyNetwork',
            'url': os.environ.get('OPEN_SKY_NETWORK_API_URL', 'https://opensky-network.org/api/states/all'),
            'params': {
                'lamin': '40.6',
                'lomin': '-74.0',
                'lamax': '40.9',
                'lomax': '-73.7'
            }
        },
        {
            'name': 'PortAuthorityNY_NJ',
            'url': os.environ.get('PORT_AUTHORITY_NY_NJ_API_URL', 'https://api.xamcheck.com/data/v1/panynj/arriving'),
            'params': {
                'token': os.environ.get('XAMCHECK_API_TOKEN', '')
            }
        }
    ]

    sources = {}
    for api in apis:
        try:
            response = requests.get(api['url'], params=api['params'])
            data = response.json()

            if api['name'] == 'OpenSkyNetwork':
                processed_data = process_open_sky_data(data)
            elif api['name'] == 'PortAuthorityNY_NJ':
                processed_data = process_port_authority_data(data)

            sources[api['name']] = {
                'status': 'success',
                'data': processed_data
            }

            # Store the processed data in DynamoDB
            store_maritime_data({
                'timestamp': datetime.utcnow().isoformat(),
                'sources': {
                    api['name']: {
                        'status': 'success',
                        'data': processed_data
                    }
                }
            })

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from {api['name']}: {str(e)}")
            sources[api['name']] = {
                'status': 'error',
                'message': str(e)
            }
            store_maritime_data({
                'timestamp': datetime.utcnow().isoformat(),
                'sources': {
                    api['name']: {
                        'status': 'error',
                        'message': str(e)
                    }
                }
            })

    return {'timestamp': datetime.utcnow().isoformat(), 'sources': sources}

def process_open_sky_data(data):
    """Process OpenSky Network data"""
    try:
        summary = {
            'total_aircraft': len(data['states']),
            'aircraft_types': {},
            'origin_countries': {}
        }
        
        for aircraft in data['states']:
            if aircraft[8] not in summary['aircraft_types']:
                summary['aircraft_types'][aircraft[8]] = 1
            else:
                summary['aircraft_types'][aircraft[8]] += 1
            
            if aircraft[2] not in summary['origin_countries']:
                summary['origin_countries'][aircraft[2]] = 1
            else:
                summary['origin_countries'][aircraft[2]] += 1
        
        return summary
    
    except Exception as e:
        logger.error(f"Error processing OpenSky Network data: {str(e)}")
        return {'error': str(e)}

def process_port_authority_data(data):
    """Process Port Authority of NY & NJ data"""
    try:
        summary = {
            'total_vessels': len(data['items']),
            'vessel_types': {}
        }
        
        for vessel in data['items']:
            if vessel['type'] not in summary['vessel_types']:
                summary['vessel_types'][vessel['type']] = 1
            else:
                summary['vessel_types'][vessel['type']] += 1
        
        return summary
    
    except Exception as e:
        logger.error(f"Error processing Port Authority of NY & NJ data: {str(e)}")
        return {'error': str(e)}

def store_maritime_data(data):
    """Store the collected data in DynamoDB"""
    try:
        item = {
            'id': str(uuid.uuid4()),
            'timestamp': data['timestamp'],
            'data': json.dumps(data['sources']),
            'status': 'processed'
        }
        
        table.put_item(Item=item)
        logger.info("Successfully stored data in DynamoDB")
        return True
    
    except Exception as e:
        logger.error(f"Error storing data in DynamoDB: {str(e)}")
        return False

def lambda_handler(event, context):
    """Main Lambda handler function"""
    try:
        logger.info("Starting maritime data collection process")
        maritime_data = fetch_maritime_data()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Maritime data ingestion complete',
                'timestamp': datetime.utcnow().isoformat(),
                'summary': {
                    source: data['status'] 
                    for source, data in maritime_data['sources'].items()
                }
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda execution error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Maritime data ingestion failed'
            })
        }