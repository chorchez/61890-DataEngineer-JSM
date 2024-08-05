import pandas as pd

from sqlalchemy import create_engine

    
class DataConn:
    def __init__(self, config: dict,schema: str):
        self.config = config
        self.schema = schema
        self.db_engine = None


    def get_conn(self):
        username = self.config.get('DDBB_USER')
        password = self.config.get('DDBB_PSW')
        host = self.config.get('DDBB_HOST')
        port = self.config.get('DDBB_PORT', '5439')
        dbname = self.config.get('DDBB_NAME')

        # Construct the connection URL
        connection_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}"
        self.db_engine = create_engine(connection_url)

        try:
            with self.db_engine.connect() as connection:
                result = connection.execute('SELECT 1;')
            if result:
                print("coneccion ok")
                return
        except Exception as e:
            print("error al conectar con ddbb")
            raise
    
    def check_table_exists(self, table_name:str) -> bool:
        with self.db_engine.connect() as connection:
            cursor = connection.cursor
            query_checker = f"""
                SELECT 1 FROM information_schema.tables 
                WHERE  table_schema = 'jor_smg_coderhouse'
                AND    table_name   = '{table_name}';              
            """
            cursor.execute(query_checker)
            
            if not cursor.fetchone():
                print("no se pudo crear tabla")
                raise ValueError(f"No {table_name} has been created")

            print("la tabla ya existe")

    def upload_data(self, data: pd.DataFrame, table: str):
        if self.db_engine is None:
            print("Execute it before")
            self.get_conn()

        try:
            data.to_sql(
                table,
                con=self.db_engine,
                schema=self.schema,
                if_exists='append',
                index=False
            )

            print("Data from the DataFrame has been uploaded to the {self.schema}.{table} table in Redshift.")
        except Exception as e:
            print("Failed to upload data to {self.schema}.{table}:\n{e}")
            raise

    def close_conn(self):
        if self.db_engine:
            self.db_engine.dispose()
            print("Connection to Redshift closed.")
        else:
            print("No active connection to close.")

