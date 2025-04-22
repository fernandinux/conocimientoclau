import logging
from mysql.connector import Error
from database.connector import DatabaseConnection
from config.settings import Config
class DetectionModel:
    TABLE_NAME = Config.DEFAULT_NAME_TABLE
    
    def __init__(self):
        self.db = DatabaseConnection()
    
    def insert_many_detections(self, detections):
        """Insert multiple detection records in a single transaction."""
        query = f"""
        INSERT INTO {Config.DEFAULT_NAME_TABLE} 
        (attribute_id, object_id, description,confidence ,init_time, final_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        try:
            with self.db.get_connection().cursor() as cursor:
                data = [
                    (   
                        int(det['attribute_id']),
                        int(det['object_id']),
                        int(det['description']),
                        int(det['confidence']),
                        int(det['init_time']),
                        str(det['final_time'])
                    )
                    for det in detections
                ]
                cursor.executemany(query, data)
                self.db.get_connection().commit()
        except Error as e:
            logging.error(f"Error inserting multiple detections: {e}")
            self.db.get_connection().rollback()
            raise