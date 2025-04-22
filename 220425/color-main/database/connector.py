import mysql.connector
import logging
from config.database import DatabaseConfig

class DatabaseConnection:
    # Variable de clase para almacenar la única instancia
    _instance = None
    
    def __new__(cls):
        """
        Método que implementa el patrón Singleton:
        - Verifica si ya existe una instancia
        - Si no existe, crea una nueva
        - Si existe, retorna la existente
        """
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
            cls._instance.initialize()
        return cls._instance
    
   def initialize(self):
        """
        Inicializa la conexión a la base de datos:
        - Crea conexión MySQL usando configuración
        - Desactiva autocommit para control de transacciones
        - Registra errores si hay problemas de conexión
        """
        try:
            self.connection = mysql.connector.connect(**DatabaseConfig.get_config())
            self.connection.autocommit = False
        except mysql.connector.Error as e:
            logging.error(f"Error al conectar a la base de datos: {e}")
            raise
    
    def get_connection(self):
        """
        Obtiene la conexión activa:
        - Verifica si la conexión está activa
        - Reinicializa si está desconectada
        - Retorna la conexión
        """
        if not self.connection.is_connected():
            self.initialize()
        return self.connection