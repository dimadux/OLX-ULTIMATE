import pymongo

from pymongo.errors import DuplicateKeyError
from scrapy.conf import settings


class MongoDBPipeline(object):

    def __init__(self):
        connection = pymongo.MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]

    def process_item(self, item, spider):
        try:
            self.collection.insert_one(item)
        except DuplicateKeyError:
            doc = self.collection.find_one(item["_id"])
            views_list = doc.get("views_list", [])
            time_list = doc.get("time_list", [])
            views_list.append(item["views_count"])
            time_list.append(item["parse_time"])
            self.collection.update_one({"_id":item["_id"]},{"$set":{
                "views_list":views_list,
                "time_list":time_list,
            }})
