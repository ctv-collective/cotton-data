from flask import Flask, request, jsonify
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import certifi
import requests

app = Flask(__name__)

# Replace the following with your actual MongoDB connection string
mongo_uri = "mongodb+srv://soylucanokan:IQ88uoMb8SCplzip@cluster0.79nwm.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

def connect_to_mongo(uri):
    try:
        client = MongoClient(uri, tlsCAFile=certifi.where())
        client.admin.command('ping')
        print("Connected to MongoDB Atlas successfully!")
        return client

    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB Atlas: {e}")
        
def get_parcel_data(lat, lon):    
    url = f'https://cbsapi.tkgm.gov.tr/megsiswebapi.v3/api/parsel/{lat}/{lon}'
        
    try:
        response = requests.get(url)
        response_code = response.status_code
    except Exception as e:
        print(f"Error fetching data for {lat}-{lon}: {str(e)}")
        result = {'error': str(e)}
    
    if response_code == 200:
        obj_json = response.json()
        properties = obj_json.get('properties', {})
        
        result = {
            'lat': lat,
            'lon': lon,
            'ilAd': properties.get('ilAd'),
            'ilceAd': properties.get('ilceAd'),
            'mevkii': properties.get('mevkii'),
            'zeminKmdurum': properties.get('zeminKmdurum'),
            'nitelik': properties.get('nitelik'),
            'mahalleAd': properties.get('mahalleAd'),
            'alan': properties.get('alan'),
            'adaNo': properties.get('adaNo'),
            'parselNo': properties.get('parselNo'),
            'pafta': properties.get('pafta'),
            'ozet' : properties.get('ozet'),
        }
    else:
        # Handle non-200 responses
        result = {'error': response.text}
    return result

# Initialize MongoDB client
client = connect_to_mongo(mongo_uri)
if client:
    db = client['cotton']
    collection = db['cotton_data']

# Endpoint to insert data into the collection
@app.route('/insert', methods=['POST'])
def insert_data():
    try:
        data = request.json
        content = data.get('content', '')
        fields = content.split(' ')
        
        if len(fields) < 8:
            return jsonify({"error": "The 'content' field must contain minimum 8 fields"}), 400

        areaCode, plateNumber, weight, weightUnit, humidity, hemisphere, lat, lon = fields

        parcel_data = get_parcel_data(lat, lon)
        
        data = {
            'areaCode': areaCode,
            'plateNumber': plateNumber,
            'weight': weight,
            'weightUnit': weightUnit,
            'humidity': humidity,
            'hemisphere': hemisphere,
            'lat': lat,
            'lon': lon,
            'city': parcel_data['ilAd'],
            'county': parcel_data['ilceAd'],
            'parcelIslandNo': parcel_data['parselNo'] + '/' +  parcel_data['adaNo'],
            'parcelArea' : parcel_data['alan'],
            'locCode' : parcel_data['ozet']
        }
        
        result = collection.insert_one(data)
        return jsonify({"success": True, "id": str(result.inserted_id)}), 201
    except Exception as e:
        print(e)
        return jsonify({"success": False, "error": str(e)}), 400

# Endpoint to retrieve all documents from the collection
@app.route('/data', methods=['GET'])
def get_data():
    try:
        all_data = list(collection.find())
        for data in all_data:
            data['_id'] = str(data['_id'])
        return jsonify(all_data), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400
    
if __name__ == "__main__":
    app.run(port=5001, debug=True)