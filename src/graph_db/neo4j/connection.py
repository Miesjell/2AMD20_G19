"""
Neo4j database connection module for the food knowledge graph.
"""
import subprocess
import time
from typing import Optional, Dict, Any, List

from neo4j import GraphDatabase, Driver, Record
import pandas as pd


def start_neo4j_docker(compose_file: str = 'docker-compose.yml', project_name: str = 'food-kg') -> bool:
    """
    Start the Neo4j Docker container using Docker Compose.
    
    Args:
        compose_file: Path to the docker-compose.yml file
        project_name: Name for the Docker Compose project
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Check if Docker is installed
        subprocess.run(['docker', '--version'], check=True, capture_output=True)
        

            # Fall back to docker-compose
        try:
            subprocess.run(['docker-compose', '--version'], check=True, capture_output=True)
            base_cmd = ['docker-compose']
            print("Using docker-compose (classic syntax)")
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("Error: Docker Compose not found.")
            return False
        
        # Build the command
        cmd = base_cmd + ['up', '-d']

        
        # Execute the command
        process = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("Docker container started successfully!")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Error starting Docker container: {e}")
        if e.stderr:
            print(f"Error details: {e.stderr}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


def stop_neo4j_docker(compose_file: str = 'docker-compose.yml', project_name: str = 'food-kg') -> bool:
    """
    Stop the Neo4j Docker container using Docker Compose.
    
    Args:
        compose_file: Path to the docker-compose.yml file
        project_name: Name for the Docker Compose project
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:

        base_cmd = ['docker-compose']
    
        # Build the command
        cmd = base_cmd + ['-f', compose_file, '-p', project_name, 'down']
        
        # Execute the command
        process = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("Docker container stopped successfully!")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Error stopping Docker container: {e}")
        if e.stderr:
            print(f"Error details: {e.stderr}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


class Neo4jConnection:
    """
    Class to manage Neo4j database connections and operations.
    """
    
    def __init__(self, uri: str = "bolt://localhost:7687", 
                 user: str = "neo4j", 
                 password: str = "password",
                 database: str = None):
        """
        Initialize the Neo4j connection.
        
        Args:
            uri: Neo4j server URI
            user: Username for authentication
            password: Password for authentication
            database: Database name (None for default)
        """
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.driver = None
    
    def connect(self) -> bool:
        """
        Establish connection to the Neo4j database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.driver = GraphDatabase.driver(
                self.uri, 
                auth=(self.user, self.password)
            )
            # Verify connectivity
            self.driver.verify_connectivity()
            print(f"Successfully connected to Neo4j at {self.uri}")
            return True
        except Exception as e:
            print(f"Failed to connect to Neo4j: {e}")
            return False
    
    def close(self) -> None:
        """Close the Neo4j connection."""
        if self.driver:
            self.driver.close()
            print("Neo4j connection closed.")
            self.driver = None
    
    def get_driver(self) -> Optional[Driver]:
        """
        Get the Neo4j driver instance.
        
        Returns:
            Driver or None: Neo4j driver if connected, None otherwise
        """
        return self.driver
    
    def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Record]:
        """
        Execute a Cypher query and return the results.
        
        Args:
            query: Cypher query string
            params: Query parameters
            
        Returns:
            List of Neo4j Records
        """
        if not self.driver:
            print("Not connected to Neo4j. Call connect() first.")
            return []
        
        with self.driver.session(database=self.database) as session:
            result = session.run(query, parameters=params or {})
            return list(result)
    
    def execute_query_to_df(self, query: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        """
        Execute a Cypher query and return results as a DataFrame.
        
        Args:
            query: Cypher query string
            params: Query parameters
            
        Returns:
            pandas.DataFrame: Query results as a DataFrame
        """
        if not self.driver:
            print("Not connected to Neo4j. Call connect() first.")
            return pd.DataFrame()
        
        with self.driver.session(database=self.database) as session:
            result = session.run(query, parameters=params or {})
            records = result.data()
            if not records:
                return pd.DataFrame()
            return pd.DataFrame(records)
    
    def check_connection(self) -> Dict[str, Any]:
        """
        Check the Neo4j connection and return database information.
        
        Returns:
            Dictionary with connection status and database info
        """
        if not self.driver:
            return {"status": "disconnected"}
        
        try:
            with self.driver.session(database=self.database) as session:
                # Get Neo4j version
                result = session.run("CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition")
                record = result.single()
                
                # Get database status
                db_result = session.run("SHOW DATABASES")
                databases = []
                for db_record in db_result:
                    databases.append({
                        'name': db_record['name'],
                        'address': db_record['address'],
                        'role': db_record['role'],
                        'status': db_record['currentStatus']
                    })
                
                return {
                    "status": "connected",
                    "database": record['name'],
                    "version": record['versions'][0],
                    "edition": record['edition'],
                    "databases": databases
                }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    def __enter__(self):
        """Context manager entry method."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit method."""
        self.close()