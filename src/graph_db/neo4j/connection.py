"""
Neo4j database connection module for the food knowledge graph.

This module provides functionality for connecting to a Neo4j database, 
managing Neo4j Docker containers, and executing queries.
"""

import subprocess
import time
from typing import Optional, Dict, Any, List, Tuple

from neo4j import GraphDatabase, Driver, Record
import pandas as pd


def start_neo4j_docker(compose_file: str = "docker-compose.yml") -> bool:
    """
    Start Neo4j Docker container using docker-compose.
    
    This function attempts to start a Neo4j container using Docker Compose.
    It supports both Docker Compose v1 (docker-compose) and v2 (docker compose).
    
    Args:
        compose_file: Path to docker-compose.yml file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # First, check if Docker Compose v2 is available (part of Docker CLI)
        is_compose_v2 = False
        try:
            result = subprocess.run(
                ["docker", "compose", "version"], 
                capture_output=True, 
                check=False, 
                text=True
            )
            is_compose_v2 = result.returncode == 0
        except Exception:
            # If the check fails, assume v1 is available
            pass
        
        # Build the appropriate command based on version
        if is_compose_v2:
            cmd = ["docker", "compose", "-f", compose_file, "up", "-d"]
        else:
            cmd = ["docker-compose", "-f", compose_file, "up", "-d"]
        
        # Execute the command
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("Neo4j Docker container started successfully!")
        
        # Wait a moment to ensure container is properly started
        time.sleep(2)
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Error starting Docker container: {e}")
        if hasattr(e, 'stderr') and e.stderr:
            print(f"Error details: {e.stderr}")
        return False
    except FileNotFoundError:
        print("Docker or docker-compose command not found. Please ensure Docker is installed.")
        return False


def stop_neo4j_docker(
    compose_file: str = "docker-compose.yml", project_name: str = "food-kg"
) -> bool:
    """
    Stop the Neo4j Docker container using Docker Compose.

    Args:
        compose_file: Path to the docker-compose.yml file
        project_name: Name for the Docker Compose project

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Determine if Docker Compose v2 is available
        is_compose_v2 = False
        try:
            result = subprocess.run(
                ["docker", "compose", "version"], 
                capture_output=True, 
                check=False, 
                text=True
            )
            is_compose_v2 = result.returncode == 0
        except Exception:
            pass
            
        # Build the appropriate command
        if is_compose_v2:
            cmd = ["docker", "compose", "-f", compose_file, "-p", project_name, "down"]
        else:
            cmd = ["docker-compose", "-f", compose_file, "-p", project_name, "down"]

        # Execute the command
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("Neo4j Docker container stopped successfully!")
        return True

    except subprocess.CalledProcessError as e:
        print(f"Error stopping Docker container: {e}")
        if hasattr(e, 'stderr') and e.stderr:
            print(f"Error details: {e.stderr}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


def check_neo4j_docker_running() -> Tuple[bool, str]:
    """
    Check if Neo4j Docker container is running.
    
    Returns:
        Tuple[bool, str]: (is_running, status_message)
    """
    try:
        # List containers using docker command
        cmd = ["docker", "ps", "--filter", "name=neo4j", "--format", "{{.Status}}"]
        result = subprocess.run(cmd, capture_output=True, check=True, text=True)
        
        # Check output to determine if Neo4j is running
        if "Up" in result.stdout:
            return True, "Neo4j container is running"
        else:
            return False, "Neo4j container is not running"
    except subprocess.CalledProcessError as e:
        return False, f"Error checking Neo4j container status: {e}"
    except FileNotFoundError:
        return False, "Docker command not found"


class Neo4jConnection:
    """
    Class to manage Neo4j database connections and operations.
    
    This class provides methods for connecting to Neo4j, executing queries,
    and retrieving information about the database.
    """

    def __init__(
        self,
        uri: str = "bolt://localhost:7687",
        user: str = "neo4j",
        password: str = "password",
        database: str = None,
    ):
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
        self.connected = False

    def connect(self, max_retries: int = 3, retry_delay: int = 2) -> bool:
        """
        Establish connection to the Neo4j database with retry functionality.

        Args:
            max_retries: Maximum number of connection attempts
            retry_delay: Delay between retries in seconds
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.driver = GraphDatabase.driver(
                    self.uri, auth=(self.user, self.password)
                )
                # Verify connectivity
                self.driver.verify_connectivity()
                self.connected = True
                print(f"Successfully connected to Neo4j at {self.uri}")
                return True
                
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"Failed to connect to Neo4j after {max_retries} attempts: {e}")
                    return False
                
                print(f"Connection attempt {retry_count} failed: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
        
        return False

    def close(self) -> None:
        """Close the Neo4j connection."""
        if self.driver:
            self.driver.close()
            print("Neo4j connection closed.")
            self.driver = None
            self.connected = False

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

    def execute_query_to_df(
        self, query: str, params: Dict[str, Any] = None
    ) -> pd.DataFrame:
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
                result = session.run(
                    "CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition"
                )
                record = result.single()

                # Get database status
                db_result = session.run("SHOW DATABASES")
                databases = []
                for db_record in db_result:
                    databases.append(
                        {
                            "name": db_record["name"],
                            "address": db_record["address"],
                            "role": db_record["role"],
                            "status": db_record["currentStatus"],
                        }
                    )

                return {
                    "status": "connected",
                    "database": record["name"],
                    "version": record["versions"][0],
                    "edition": record["edition"],
                    "databases": databases,
                }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def __enter__(self):
        """Context manager entry method."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit method."""
        self.close()
