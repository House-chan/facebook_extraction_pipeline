from apscheduler.schedulers.blocking import BlockingScheduler
import logging
from datetime import datetime
from apify_client import ApifyClient
import re
import Extraction_model
from dateutil import parser
import os
import dotenv
dotenv.load_dotenv()
import pymongo

import requests
from PIL import Image
from io import BytesIO

import cloudinary
import cloudinary.uploader

cloudinary_api_key = os.getenv('CLOUDINARY_API_KEY')
cloudinary_secret_key = os.getenv('CLOUDINARY_SECRET_KEY')
cloudinary.config( 
    cloud_name = "dlfnuixd0", 
    api_key = cloudinary_api_key, 
    api_secret = cloudinary_secret_key, # Click 'View Credentials' below to copy your API secret
    secure=True
)

logging.basicConfig(level=logging.INFO)

mongo = os.getenv('MONGODB_KEY')
uri = f"mongodb+srv://housechan:{mongo}@cluster0.wl8mbpy.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = pymongo.MongoClient(uri)
db = client["real_estate_thai"]
properties = db["house_properties"]

APIFY_API_KEY = os.getenv('APIFY_API_KEY')
client = ApifyClient(APIFY_API_KEY)

#? previous list for initiate
previous_id = "159hALfBmD4OrehG0"
previous_house_list = []
for item in client.dataset(previous_id).iterate_items():
    previous_house_list.append(item)
house_list = []

def extract_data():
    # Extract data from your source (e.g., database, API)
     run_input = {
          "startUrls": [{ "url": "https://www.facebook.com/share/p/rktZjXuX8avXvKJe/?mibextid=K35XfP" }],
          "resultsLimit": 100,
          "viewOption": "CHRONOLOGICAL",
     }
     run = client.actor("2chN8UQcH1CfxLRNE").call(run_input=run_input)
     current_id = run["defaultDatasetId"]
     logging.info("Scraping completed")
    #? current list
     house_list = []
     for item in client.dataset(current_id).iterate_items():
         if any(item["url"] in house["url"] for house in previous_house_list):
             break
         else:
             house_list.append(item)
     logging.info(f"Exclude duplicate {500 - len(house_list)} items completed")

     return house_list

def transform_and_upload_data(house_list):
    extraction_list = []
    count = 0
    unit_id = get_unit_id()
    for i, doc in enumerate(house_list):
        img_url = []
        date = parser.parse(doc["time"])
        text = doc["text"]
        #? normal post
        if "attachments" in doc:
          for attachment in doc["attachments"][1:]:
               img_url.append(attachment["thumbnail"])
        #? Shared post
        elif "sharedPost" in doc:
            # for media in doc["sharedPost"]["media"]:
            if "media" in doc["sharedPost"]:
                for attachment in doc["sharedPost"]["media"][1:]:
                        img_url.append(attachment["thumbnail"])
            #? use date time shared post
            date = parser.parse(doc["time"])
            text = text + "\n" + doc["sharedPost"]["text"]
        
        #! Transform
        extraction = Extraction_model.get_entities(text=text, date=date)

        if "ต้องการขายบ้าน" == extraction["post_type"] and not check_dict_keys(extraction):
            del extraction["post_type"]
            unit_id = "F" + str(int(get_unit_id() + 1))
            extraction["unit_id"] = unit_id

            # change facebook img url to cloudinary
            if img_url:
                for i, url in enumerate(img_url):
                    download_webp_image(url, "test.png")
                    img_url[i] = upload_image("test.png", (unit_id+"-"+str(i)))
                extraction["img_url"] = img_url

            #! Load
            properties.insert_one(extraction)
            count += 1
        if i % 100 == 0:
            logging.info(f"Extraction and load Currently on {i}")

    # Transform the extracted data
    logging.info("Extraction and load Complete")
    logging.info(f"From {len(house_list)} posts remains in {count}")
    previous_house_list = house_list
    return extraction_list

def get_unit_id():
    #? check atleast location, price, bathroom, bedrooms have to 

        #? get last unit_id
    result = properties.find().sort("unit_id", -1).limit(1)
    for i in result:
        unit_id = i["unit_id"]
    regex = re.search(r'\d+', unit_id)
    return int(regex.group())
    # logging.info(f"Load items completed ({count})")

def delete_empty_data():
    query = {
        "location": "",
        "price": 0,
        "area_wah": 0,
        "area_meter": 0,
        "img_url": []
    }

    # Delete documents that match the query
    result = properties.delete_many(query)

    logging.info(f"Deleted {result.deleted_count} documents.")

def download_webp_image(url, save_path):
    """Downloads a WebP image from a URL and saves it to the specified path."""

    response = requests.get(url)
    response.raise_for_status()  # Check for errors (e.g., 404)

    # Open WebP directly from the response content
    with Image.open(BytesIO(response.content)) as img:
        img.thumbnail((800, 600))
        img.save(save_path) 

def upload_image(save_path, filename):
    upload_result = cloudinary.uploader.upload(save_path, public_id=filename)
    url = upload_result["secure_url"]
    return url

def check_dict_keys(d):
    for key in ["location", "price", "area_wah", "area_meter"]:
        if key in d and d[key]:
            return False
    return True

def main():
    house_list = extract_data()
    transform_and_upload_data(house_list)
    delete_empty_data()
# Create a scheduler
main()

sched = BlockingScheduler()

# Schedule tasks
sched.add_job(main, 'interval', days=1) 
  # Run task2 every day

# Start the scheduler
sched.start()