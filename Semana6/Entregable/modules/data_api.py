import requests
from io import StringIO
import pandas as pd

class DataRetriever:
    
    def get_data(self):
        response_json = requests.get("https://api.freeapi.app/api/v1/public/youtube/videos?page=1&limit=10&query=javascript&sortBy=keep%20one%3A%20mostLiked%20%7C%20mostViewed%20%7C%20latest%20%7C%20oldest").json()
        data_by_list_api:pd.DataFrame = pd.DataFrame(response_json)
        # columnas necesarias para la ingestan en las tablas
        cols:list[str] = ["id","title","description","date"]
        data = data_by_list_api[cols]
        
        try:
            data = pd.DataFrame(data)
            data = data.fillna(0)
            buffer = StringIO()
            data.info(buf=buffer)
            s = buffer.getvalue()

            return data
        
        except Exception as e:
            raise