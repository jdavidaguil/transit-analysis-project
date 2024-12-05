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
table = dynamodb.Table('transit-analysis-data')


def fetch_maritime_data():
    """Fetch data from the OpenSky Network API"""
    api_url = os.environ.get('OPEN_SKY_NETWORK_API_URL', 'https://opensky-network.org/api/states/all')
    api_params = {
        'lamin': '40.6',
        'lomin': '-74.0',
        'lamax': '40.9',
        'lomax': '-73.7'
    }

    try:
        response = requests.get(api_url, params=api_params)
        data = response.json()
        processed_data = process_open_sky_data(data)

        # Store the processed data in DynamoDB
        store_maritime_data({
            'timestamp': datetime.utcnow().isoformat(),
            'source': 'OpenSkyNetwork',
            'data': processed_data
        })

        return {
            'status': 'success',
            'data': processed_data
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from OpenSkyNetwork: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }

def process_open_sky_data(data):
    """Process OpenSky Network data"""
    try:
        aircraft_data = []
        
        for aircraft in data['states']:
            if aircraft[5] is not None and aircraft[6] is not None:
                aircraft_data.append({
                    'icao24': aircraft[0],
                    'callsign': aircraft[1],
                    'origin_country': aircraft[2],
                    'latitude': aircraft[5],
                    'longitude': aircraft[6]
                })
        
        summary = {
            'total_aircraft': len(aircraft_data),
            'aircraft_data': aircraft_data
        }
        
        return summary
    
    except Exception as e:
        logger.error(f"Error processing OpenSky Network data: {str(e)}")
        return {'error': str(e)}

def store_maritime_data(data):
    """Store the collected data in DynamoDB"""
    try:
        item = {
            'id': str(uuid.uuid4()),
            'timestamp': data['timestamp'],
            'source': data['source'],
            'data': json.dumps(data['data']),
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
                'data': maritime_data
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