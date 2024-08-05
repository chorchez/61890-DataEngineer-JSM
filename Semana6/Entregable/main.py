import os
from modules import DataConn , DataRetriever
from dotenv import load_dotenv


load_dotenv()

def main():
    user_credentials = {
        "DDBB_USER" : os.getenv('DDBB_USER'),
        "DDBB_PSW" : os.getenv('DDBB_PSW'),
        "DDBB_HOST" : os.getenv('DDBB_HOST'),
        "DDBB_PORT" : os.getenv('DDBB_PORT', '5439'),
        "DDBB_NAME" : os.getenv('DDBB_NAME')
    }

    schema:str = "jor_smg_coderhouse"
    table:str = "jsmg_01_api"

    data_conn = DataConn(user_credentials, schema)
    data_retriever = DataRetriever()

    try:
        data = data_retriever.get_data()
        data_conn.upload_data(data, table)
        print("data subida ok")

    except Exception as e:
        print("error al subir datos")
        
    finally:
        data_conn.close_conn()

if __name__ == "__main__":
    main()