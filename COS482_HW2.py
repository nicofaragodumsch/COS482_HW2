import psycopg2
from psycopg2 import errors
import getpass

def create_tables():
    """
    Create five tables (Movie, Person, ActsIn, Director, Directs) 
    in the moviesdb database with appropriate constraints.
    """
    
    # Prompt user for database password (hidden input)
    print("PostgreSQL Database Connection")
    db_password = getpass.getpass("Enter PostgreSQL password for user 'postgres': ")
    
    # Connect to the moviesdb database
    try:
        conn = psycopg2.connect(
            host="localhost",
            dbname="moviesdb",
            user="postgres",
            password=db_password
        )
        cur = conn.cursor()
        print("Successfully connected to moviesdb database")
        
        # Create Movie table
        print("\nCreating Movie table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Movie(
                id INTEGER PRIMARY KEY,
                name TEXT,
                year INTEGER,
                rank REAL
            )
        """)
        print("Movie table created successfully")
        
        # Create Person table
        print("\nCreating Person table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Person(
                id INTEGER PRIMARY KEY,
                fname TEXT,
                lname TEXT,
                gender TEXT
            )
        """)
        print("Person table created successfully")
        
        # Create Director table
        print("\nCreating Director table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Director(
                id INTEGER PRIMARY KEY,
                fname TEXT,
                lname TEXT
            )
        """)
        print("Director table created successfully")
        
        # Create ActsIn table (with foreign key constraints)
        print("\nCreating ActsIn table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ActsIn(
                pid INTEGER,
                mid INTEGER,
                role TEXT,
                PRIMARY KEY (pid, mid),
                FOREIGN KEY (pid) REFERENCES Person(id),
                FOREIGN KEY (mid) REFERENCES Movie(id)
            )
        """)
        print("ActsIn table created successfully")
        
        # Create Directs table (with foreign key constraints)
        print("\nCreating Directs table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Directs(
                did INTEGER,
                mid INTEGER,
                PRIMARY KEY (did, mid),
                FOREIGN KEY (did) REFERENCES Director(id),
                FOREIGN KEY (mid) REFERENCES Movie(id)
            )
        """)
        print("Directs table created successfully")
        
        # Commit the transaction
        conn.commit()
        print("\n✓ All tables created successfully and committed to database")
        
        # List all tables in the database
        print("\nVerifying tables in moviesdb:")
        cur.execute("""
            SELECT tablename FROM pg_catalog.pg_tables 
            WHERE schemaname = 'public'
        """)
        tables = cur.fetchall()
        for table in tables:
            print(f"  - {table[0]}")
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
    
    finally:
        # Close cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("\nDatabase connection closed")

if __name__ == "__main__":
    create_tables()