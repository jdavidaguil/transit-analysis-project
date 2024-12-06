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
            'lomin': -81.4294514581764,
            'lamin': 34.7438227809691,
            'lomax': -80.2594075128639,
            'lamax': 35.708561567598885
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
            # The 'aircraft' list contains the following elements:
            # 0. icao24
            # 1. callsign
            # 2. origin_country
            # 3. time_position
            # 4. time_velocity
            # 5. latitude
            # 6. longitude
            # 7. geo_altitude
            # 8. on_ground
            # 9. velocity
            # 10. heading
            # 11. vertical_rate
            # 12. sensors
            # 13. bar_altitude
            # 14. squawk
            # 15. spi
            # 16. position_source
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