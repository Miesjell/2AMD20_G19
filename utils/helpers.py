def print_browser_access_info(user: str, password: str) -> None:
    """
    Print information about how to access the Neo4j Browser.
    
    Args:
        user: Neo4j username
        password: Neo4j password
    """
    print("\n" + "=" * 80)
    print("  NEO4J BROWSER ACCESS")
    print("=" * 80)
    print("  You can access your Knowledge Graph at: http://localhost:7474/")
    print(f"  Username: {user}")
    print(f"  Password: {password}")
    print("=" * 80)
    print("  To explore your graph, try these Cypher queries:")
    print("  MATCH (n) RETURN n LIMIT 25")
    print(
        "  MATCH (p:Person)-[:RECOMMENDED_RECIPE]->(r:Recipe) RETURN p, r LIMIT 10"
    )
    print("=" * 80)
    
    
    
def parse_args():
    """Parse command line arguments with descriptions."""
    parser = argparse.ArgumentParser(
        description="Food Knowledge Graph Manager",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Data source parameters
    parser.add_argument(
        "--data-dir", 
        default="data", 
        help="Directory containing data files"
    )
    
    # Connection parameters
    parser.add_argument(
        "--docker-compose",
        default="docker-compose.yml",
        help="Path to docker-compose.yml file",
    )
    parser.add_argument(
        "--uri", 
        default="bolt://localhost:7687", 
        help="Neo4j connection URI"
    )
    parser.add_argument(
        "--user", 
        default="neo4j", 
        help="Neo4j username"
    )
    parser.add_argument(
        "--password", 
        default="password", 
        help="Neo4j password"
    )
    
    # Docker control parameters
    parser.add_argument(
        "--start-docker", 
        action="store_true", 
        help="Start Neo4j Docker container"
    )
    parser.add_argument(
        "--stop-docker", 
        action="store_true", 
        help="Stop Neo4j Docker container"
    )
    
    # Data sampling parameters
    parser.add_argument(
        "--sample-recipes", 
        type=int, 
        default=10000, 
        help="Number of recipes to sample"
    )
    parser.add_argument(
        "--sample-persons", 
        type=int, 
        default=10000, 
        help="Number of persons to sample"
    )
    
    # Skip-step parameters
    parser.add_argument(
        "--skip-schema", 
        action="store_true", 
        help="Skip schema setup"
    )
    parser.add_argument(
        "--skip-food", 
        action="store_true", 
        help="Skip food data loading"
    )
    parser.add_argument(
        "--skip-recipes", 
        action="store_true", 
        help="Skip recipe data loading"
    )
    parser.add_argument(
        "--skip-persons", 
        action="store_true", 
        help="Skip person data loading"
    )
    parser.add_argument(
        "--skip-relationships", 
        action="store_true", 
        help="Skip relationship creation"
    )
    parser.add_argument(
        "--skip-queries", 
        action="store_true", 
        help="Skip running verification queries"
    )

    return parser.parse_args()
