import json
import boto3
import requests
from datetime import datetime
import uuid
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('transit-analysis-data')


def fetch_transit_data():
    """Fetch data from multiple transit APIs"""
    apis = [
        {
            'name': 'NYC_Traffic',
            'url': os.environ.get('NYC_TRAFFIC_API_URL', 'https://data.cityofnewyork.us/resource/i4gi-tjb9.json'),
            'params': {
                '$limit': 10,
                '$where': 'speed IS NOT NULL'
            }
        },
        {
            'name': 'CityBikes_NYC',
            'url': os.environ.get('CITYBIKES_API_URL', 'http://api.citybik.es/v2/networks/citi-bike-nyc'),
            'params': {}
        },
        {
            'name': 'Weather_NYC',
            'url': os.environ.get('WEATHER_API_URL', 'https://api.openweathermap.org/data/2.5/weather'),
            'params': {
                'q': 'New York,US',
                'appid': os.environ.get('WEATHER_API_KEY', ''),
                'units': 'metric'
            }
        }
    ]
def process_traffic_data(data):
    """Process NYC traffic data"""
    try:
        summary = {
            'total_records': len(data),
            'average_speed': 0,
            'congested_segments': 0,
            'locations': []
        }
        
        total_speed = 0
        for record in data:
            speed = float(record.get('speed', 0))
            total_speed += speed
            
            if speed < 15:  # Define congestion as < 15 mph
                summary['congested_segments'] += 1
            
            summary['locations'].append({
                'speed': speed,
                'borough': record.get('borough', 'Unknown'),
                'link_name': record.get('link_name', 'Unknown')
            })
        
        if data:
            summary['average_speed'] = total_speed / len(data)
        
        return summary
    
    except Exception as e:
        logger.error(f"Error processing traffic data: {str(e)}")
        return {'error': str(e)}

def process_bike_data(data):
    """Process CitiBike data"""
    try:
        network = data.get('network', {})
        stations = network.get('stations', [])
        
        summary = {
            'total_stations': len(stations),
            'available_bikes': 0,
            'available_docks': 0,
            'stations_data': []
        }
        
        for station in stations[:10]:  # Limit to 10 stations for summary
            bikes = station.get('free_bikes', 0)
            docks = station.get('empty_slots', 0)
            
            summary['available_bikes'] += bikes
            summary['available_docks'] += docks
            
            summary['stations_data'].append({
                'name': station.get('name', 'Unknown'),
                'available_bikes': bikes,
                'available_docks': docks,
                'latitude': station.get('latitude'),
                'longitude': station.get('longitude')
            })
        
        return summary
    
    except Exception as e:
        logger.error(f"Error processing bike data: {str(e)}")
        return {'error': str(e)}

def store_transit_data(data):
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
        logger.info("Starting data collection process")
        transit_data = fetch_transit_data()
        
        logger.info("Storing collected data")
        storage_success = store_transit_data(transit_data)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data ingestion complete',
                'success': storage_success,
                'timestamp': datetime.utcnow().isoformat(),
                'summary': {
                    source: data['status'] 
                    for source, data in transit_data['sources'].items()
                }
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda execution error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Data ingestion failed'
            })
        }