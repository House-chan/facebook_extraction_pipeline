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
          "resultsLimit": 500,
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

def transform_and_upload_data():
    extraction_list = []
    count = 0
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
        extraction = Extraction_model.get_entities(text=text, date=date, img_list=img_url)

        if "ต้องการขายบ้าน" == extraction["post_type"] and not check_dict_keys(house):
            del extraction["post_type"]
            extraction["unit_id"] = get_unit_id(extraction)
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

def get_unit_id(house):
    #? check atleast location, price, bathroom, bedrooms have to 
    if not check_dict_keys(house):
        #? get last unit_id
        result = properties.find().sort("unit_id", -1).limit(1)
        for i in result:
            unit_id = i["unit_id"]
        regex = re.search(r'\d+', unit_id)
        unit_id = "F" + str(int(regex.group()) + 1)
        return unit_id
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

def check_dict_keys(d):
    for key in ["location", "price", "area_wah", "area_meter"]:
        if key in d and d[key]:
            return False
    return True

def main():
    extract_data()
    transform_and_upload_data()
    delete_empty_data()
# Create a scheduler
main()

sched = BlockingScheduler()

# Schedule tasks
sched.add_job(main, 'interval', days=1) 
  # Run task2 every day

# Start the scheduler
sched.start()