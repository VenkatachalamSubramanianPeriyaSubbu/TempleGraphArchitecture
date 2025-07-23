import json
from neo4j import GraphDatabase
import re
from typing import Dict, List, Any
import os

uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASSWORD")

class TempleGraphDB:
    def __init__(self, uri=uri, user=user, password=password):
        """
        Initialize connection to Neo4j database
        
        Args:
            uri: Neo4j database URI
            user: Database username
            password: Database password
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        """Close database connection"""
        self.driver.close()
    
    def clear_database(self):
        """Clear all nodes and relationships"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Database cleared!")
    
    def create_constraints(self):
        """Create unique constraints for better performance"""
        with self.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT temple_name IF NOT EXISTS FOR (t:Temple) REQUIRE t.name IS UNIQUE",
                "CREATE CONSTRAINT state_name IF NOT EXISTS FOR (s:State) REQUIRE s.name IS UNIQUE",
                "CREATE CONSTRAINT deity_name IF NOT EXISTS FOR (d:Deity) REQUIRE d.name IS UNIQUE",
                "CREATE CONSTRAINT scripture_name IF NOT EXISTS FOR (sc:Scripture) REQUIRE sc.name IS UNIQUE",
                "CREATE CONSTRAINT architectural_style IF NOT EXISTS FOR (a:ArchitecturalStyle) REQUIRE a.name IS UNIQUE",
                "CREATE CONSTRAINT festival_name IF NOT EXISTS FOR (f:Festival) REQUIRE f.name IS UNIQUE"
            ]
            
            for constraint in constraints:
                try:
                    session.run(constraint)
                    print(f"Created constraint: {constraint.split('FOR')[1].split('REQUIRE')[0].strip()}")
                except Exception as e:
                    print(f"Constraint may already exist: {e}")
    
    def extract_deities(self, text: str) -> List[str]:
        """Extract deity names from temple information"""
        if not text:
            return []
        
        # Common deity patterns
        deity_patterns = [
            r'(?:Lord|Goddess|Sri|Shri)\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)',
            r'([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)\s+(?:Temple|Swamy|Amman)',
            r'dedicated to\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)',
            r'deity[:\s]+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)'
        ]
        
        deities = set()
        for pattern in deity_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if len(match.strip()) > 2:  # Filter out very short matches
                    deities.add(match.strip().title())
        
        # Common deity names to look for
        common_deities = [
            'Shiva', 'Vishnu', 'Krishna', 'Rama', 'Jagannath', 'Venkateswara', 
            'Balaji', 'Meenakshi', 'Parvati', 'Lakshmi', 'Saraswati', 'Ganesha',
            'Hanuman', 'Murugan', 'Subhadra', 'Balabhadra', 'Sundareshwara'
        ]
        
        for deity in common_deities:
            if deity.lower() in text.lower():
                deities.add(deity)
        
        return list(deities)
    
    def extract_scriptures(self, text: str) -> List[str]:
        """Extract scripture references from text"""
        if not text:
            return []
        
        scripture_patterns = [
            r'([A-Z][a-zA-Z]+)\s+Purana',
            r'(Mahabharata|Ramayana|Bhagavad Gita|Vedas?)',
            r'(Skanda Purana|Padma Purana|Varaha Purana|Bhagavata Purana)'
        ]
        
        scriptures = set()
        for pattern in scripture_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                scriptures.add(match.strip().title())
        
        return list(scriptures)
    
    def extract_architectural_style(self, text: str) -> List[str]:
        """Extract architectural styles from text"""
        if not text:
            return []
        
        styles = []
        if 'dravidian' in text.lower():
            styles.append('Dravidian')
        if 'kalinga' in text.lower():
            styles.append('Kalinga')
        if 'chola' in text.lower():
            styles.append('Chola')
        if 'pallava' in text.lower():
            styles.append('Pallava')
        if 'vijayanagara' in text.lower():
            styles.append('Vijayanagara')
        
        return styles
    
    def extract_festivals(self, text: str) -> List[str]:
        """Extract festival names from text"""
        if not text:
            return []
        
        festival_patterns = [
            r'([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)\s+festival',
            r'festival[:\s]+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)',
            r'(Brahmotsavam|Navaratri|Rath Yatra|Tirukalyanam)'
        ]
        
        festivals = set()
        for pattern in festival_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if len(match.strip()) > 3:
                    festivals.add(match.strip().title())
        
        return list(festivals)
    
    def create_temple_node(self, temple_data: Dict[str, Any]):
        """Create a temple node with its properties"""
        with self.driver.session() as session:
            # Create temple node
            temple_query = """
            CREATE (t:Temple {
                name: $name,
                state: $state,
                info: $info,
                story: $story,
                visiting_guide: $visiting_guide,
                architecture: $architecture,
                scripture_mentions: $scripture_mentions
            })
            RETURN t
            """
            
            session.run(temple_query, 
                       name=temple_data.get('name', ''),
                       state=temple_data.get('state', ''),
                       info=temple_data.get('info', ''),
                       story=temple_data.get('story', ''),
                       visiting_guide=temple_data.get('visiting_guide', ''),
                       architecture=temple_data.get('architecture', ''),
                       scripture_mentions=temple_data.get('mention_in_scripture', ''))
            
            print(f"Created temple node: {temple_data.get('name', 'Unknown')}")
    
    def create_relationships(self, temple_data: Dict[str, Any]):
        """Create relationships between temple and other entities"""
        temple_name = temple_data.get('name', '')
        state_name = temple_data.get('state', '')
        
        # Combine all text for entity extraction
        all_text = ' '.join([
            temple_data.get('info', ''),
            temple_data.get('story', ''),
            temple_data.get('architecture', ''),
            temple_data.get('mention_in_scripture', '')
        ])
        
        with self.driver.session() as session:
            # Create state relationship
            if state_name:
                session.run("""
                    MERGE (s:State {name: $state_name})
                    WITH s
                    MATCH (t:Temple {name: $temple_name})
                    MERGE (t)-[:LOCATED_IN]->(s)
                """, state_name=state_name, temple_name=temple_name)
            
            # Create deity relationships
            deities = self.extract_deities(all_text)
            for deity in deities:
                session.run("""
                    MERGE (d:Deity {name: $deity_name})
                    WITH d
                    MATCH (t:Temple {name: $temple_name})
                    MERGE (t)-[:DEDICATED_TO]->(d)
                """, deity_name=deity, temple_name=temple_name)
            
            # Create scripture relationships
            scriptures = self.extract_scriptures(all_text)
            for scripture in scriptures:
                session.run("""
                    MERGE (sc:Scripture {name: $scripture_name})
                    WITH sc
                    MATCH (t:Temple {name: $temple_name})
                    MERGE (t)-[:MENTIONED_IN]->(sc)
                """, scripture_name=scripture, temple_name=temple_name)
            
            # Create architectural style relationships
            styles = self.extract_architectural_style(all_text)
            for style in styles:
                session.run("""
                    MERGE (a:ArchitecturalStyle {name: $style_name})
                    WITH a
                    MATCH (t:Temple {name: $temple_name})
                    MERGE (t)-[:HAS_STYLE]->(a)
                """, style_name=style, temple_name=temple_name)
            
            # Create festival relationships
            festivals = self.extract_festivals(all_text)
            for festival in festivals:
                session.run("""
                    MERGE (f:Festival {name: $festival_name})
                    WITH f
                    MATCH (t:Temple {name: $temple_name})
                    MERGE (t)-[:CELEBRATES]->(f)
                """, festival_name=festival, temple_name=temple_name)
            
            print(f"Created relationships for: {temple_name}")
            print(f"  - Deities: {deities}")
            print(f"  - Scriptures: {scriptures}")
            print(f"  - Styles: {styles}")
            print(f"  - Festivals: {festivals}")
    
    def load_temple_data(self, json_file_path: str):
        """Load temple data from JSON file and create graph"""
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            def replace_none(obj):
                if isinstance(obj, dict):
                    return {k: replace_none(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [replace_none(elem) for elem in obj]
                else:
                    return '' if obj is None else obj
            
            data = replace_none(data)  # Replace None with empty strings

            # Clear existing data
            self.clear_database()
            
            # Create constraints
            self.create_constraints()
            
            # Process each state's temples
            for state, temples in data.items():
                print(f"\nProcessing temples in {state}...")
                
                for temple in temples:
                    # Create temple node
                    self.create_temple_node(temple)
                    
                    # Create relationships
                    self.create_relationships(temple)
            
            print("\n=== Graph Database Creation Complete! ===")
            self.print_statistics()
            
        except Exception as e:
            print(f"Error loading data: {e}")
    
    def print_statistics(self):
        """Print database statistics"""
        with self.driver.session() as session:
            stats = {
                'Temples': session.run("MATCH (t:Temple) RETURN count(t) as count").single()['count'],
                'States': session.run("MATCH (s:State) RETURN count(s) as count").single()['count'],
                'Deities': session.run("MATCH (d:Deity) RETURN count(d) as count").single()['count'],
                'Scriptures': session.run("MATCH (sc:Scripture) RETURN count(sc) as count").single()['count'],
                'Architectural Styles': session.run("MATCH (a:ArchitecturalStyle) RETURN count(a) as count").single()['count'],
                'Festivals': session.run("MATCH (f:Festival) RETURN count(f) as count").single()['count'],
                'Total Relationships': session.run("MATCH ()-[r]->() RETURN count(r) as count").single()['count']
            }
            
            print("\n=== Database Statistics ===")
            for key, value in stats.items():
                print(f"{key}: {value}")
    
    def run_sample_queries(self):
        """Run some sample queries to demonstrate the graph"""
        with self.driver.session() as session:
            print("\n=== Sample Queries ===")
            
            # Query 1: Find all temples dedicated to Vishnu
            print("\n1. Temples dedicated to Vishnu:")
            result = session.run("""
                MATCH (t:Temple)-[:DEDICATED_TO]->(d:Deity)
                WHERE d.name CONTAINS 'Vishnu' OR d.name CONTAINS 'Venkateswara'
                RETURN t.name, d.name
            """)
            for record in result:
                print(f"   {record['t.name']} -> {record['d.name']}")
            
            # Query 2: Find temples by architectural style
            print("\n2. Temples with Dravidian architecture:")
            result = session.run("""
                MATCH (t:Temple)-[:HAS_STYLE]->(a:ArchitecturalStyle {name: 'Dravidian'})
                RETURN t.name
            """)
            for record in result:
                print(f"   {record['t.name']}")
            
            # Query 3: Find temples mentioned in scriptures
            print("\n3. Temples mentioned in Puranas:")
            result = session.run("""
                MATCH (t:Temple)-[:MENTIONED_IN]->(sc:Scripture)
                WHERE sc.name CONTAINS 'Purana'
                RETURN t.name, sc.name
            """)
            for record in result:
                print(f"   {record['t.name']} -> {record['sc.name']}")
            
            # Query 4: Find temples in a specific state
            print("\n4. Temples in Andhra Pradesh:")
            result = session.run("""
                MATCH (t:Temple)-[:LOCATED_IN]->(s:State {name: 'Andhra Pradesh'})
                RETURN t.name
            """)
            for record in result:
                print(f"   {record['t.name']}")

def main():
    """Main function to demonstrate usage"""
    # Initialize database connection
    db = TempleGraphDB()

    try:
        # Load data from JSON file (update path as needed)
        db.load_temple_data('data/hindu_temples.json')  # Your JSON file
        
        # Run sample queries
        db.run_sample_queries()
        
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure Neo4j is installed and running on bolt://localhost:7687")
        print("Default credentials: neo4j/password")
    
    finally:
        db.close()

if __name__ == "__main__":
    main()