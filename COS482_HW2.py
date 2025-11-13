import psycopg2
from psycopg2 import errors
import getpass
import os

def create_tables_and_load_data():
    """
    Create five tables (Movie, Person, ActsIn, Director, Directs) 
    in the moviesdb database and load data from IMDB text files.
    """
    
    # Prompt user for database password (hidden input)
    print("PostgreSQL Database Connection")
    db_password = getpass.getpass("Enter PostgreSQL password for user 'postgres': ")
    
    # Prompt user for directory containing IMDB files
    data_dir = input("Enter the path to the directory containing IMDB .txt files: ").strip()
    
    if not os.path.isdir(data_dir):
        print(f"Error: Directory '{data_dir}' does not exist")
        return
    
    # Connect to the moviesdb database
    try:
        conn = psycopg2.connect(
            host="localhost",
            dbname="moviesdb",
            user="postgres",
            password=db_password
        )
        cur = conn.cursor()
        print("Successfully connected to moviesdb database\n")
        
        # Drop existing tables if they exist (to allow re-running)
        print("Dropping existing tables if they exist...")
        cur.execute("DROP TABLE IF EXISTS ActsIn CASCADE")
        cur.execute("DROP TABLE IF EXISTS Directs CASCADE")
        cur.execute("DROP TABLE IF EXISTS Movie CASCADE")
        cur.execute("DROP TABLE IF EXISTS Person CASCADE")
        cur.execute("DROP TABLE IF EXISTS Director CASCADE")
        conn.commit()
        
        # Create Movie table
        print("Creating Movie table...")
        cur.execute("""
            CREATE TABLE Movie(
                id INTEGER PRIMARY KEY,
                name TEXT,
                year INTEGER,
                rank REAL
            )
        """)
        conn.commit()
        print("Movie table created successfully")
        
        # Create Person table
        print("Creating Person table...")
        cur.execute("""
            CREATE TABLE Person(
                id INTEGER PRIMARY KEY,
                fname TEXT,
                lname TEXT,
                gender TEXT
            )
        """)
        conn.commit()
        print("Person table created successfully")
        
        # Create Director table
        print("Creating Director table...")
        cur.execute("""
            CREATE TABLE Director(
                id INTEGER PRIMARY KEY,
                fname TEXT,
                lname TEXT
            )
        """)
        conn.commit()
        print("Director table created successfully")
        
        # Create ActsIn table (with foreign key constraints)
        print("Creating ActsIn table...")
        cur.execute("""
            CREATE TABLE ActsIn(
                pid INTEGER,
                mid INTEGER,
                role TEXT,
                PRIMARY KEY (pid, mid),
                FOREIGN KEY (pid) REFERENCES Person(id),
                FOREIGN KEY (mid) REFERENCES Movie(id)
            )
        """)
        conn.commit()
        print("ActsIn table created successfully")
        
        # Create Directs table (with foreign key constraints)
        print("Creating Directs table...")
        cur.execute("""
            CREATE TABLE Directs(
                did INTEGER,
                mid INTEGER,
                PRIMARY KEY (did, mid),
                FOREIGN KEY (did) REFERENCES Director(id),
                FOREIGN KEY (mid) REFERENCES Movie(id)
            )
        """)
        conn.commit()
        print("Directs table created successfully\n")
        
        # Load data from IMDB files
        print("=" * 60)
        print("Loading data from IMDB files...")
        print("=" * 60)
        
        # Load Movie data from IMDBMovie.txt
        movie_file = os.path.join(data_dir, "IMDBMovie.txt")
        print(f"\nLoading Movie data from {movie_file}...")
        movie_count = 0
        movie_skipped = 0
        
        try:
            with open(movie_file, 'r', encoding='latin-1') as f:
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) >= 4:
                        try:
                            movie_id = int(parts[0])
                            name = parts[1]
                            year = int(parts[2]) if parts[2] else None
                            rank = float(parts[3]) if parts[3] else None
                            
                            cur.execute(
                                "INSERT INTO Movie (id, name, year, rank) VALUES (%s, %s, %s, %s)",
                                (movie_id, name, year, rank)
                            )
                            conn.commit()
                            movie_count += 1
                        except (ValueError, errors.UniqueViolation, psycopg2.Error) as e:
                            movie_skipped += 1
                            conn.rollback()
                            continue
            print(f"✓ Loaded {movie_count} movies ({movie_skipped} skipped due to errors)")
        except FileNotFoundError:
            print(f"✗ File not found: {movie_file}")
        
        # Load Person data from IMDBPerson.txt
        person_file = os.path.join(data_dir, "IMDBPerson.txt")
        print(f"\nLoading Person data from {person_file}...")
        person_count = 0
        person_skipped = 0
        
        try:
            with open(person_file, 'r', encoding='latin-1') as f:
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) >= 4:
                        try:
                            person_id = int(parts[0])
                            fname = parts[1]
                            lname = parts[2]
                            gender = parts[3]
                            
                            cur.execute(
                                "INSERT INTO Person (id, fname, lname, gender) VALUES (%s, %s, %s, %s)",
                                (person_id, fname, lname, gender)
                            )
                            conn.commit()
                            person_count += 1
                        except (ValueError, errors.UniqueViolation, psycopg2.Error) as e:
                            person_skipped += 1
                            conn.rollback()
                            continue
            print(f"✓ Loaded {person_count} persons ({person_skipped} skipped due to errors)")
        except FileNotFoundError:
            print(f"✗ File not found: {person_file}")
        
        # Load Director data from IMDBDirectors.txt
        director_file = os.path.join(data_dir, "IMDBDirectors.txt")
        print(f"\nLoading Director data from {director_file}...")
        director_count = 0
        director_skipped = 0
        
        try:
            with open(director_file, 'r', encoding='latin-1') as f:
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) >= 3:
                        try:
                            director_id = int(parts[0])
                            fname = parts[1]
                            lname = parts[2]
                            
                            cur.execute(
                                "INSERT INTO Director (id, fname, lname) VALUES (%s, %s, %s)",
                                (director_id, fname, lname)
                            )
                            conn.commit()
                            director_count += 1
                        except (ValueError, errors.UniqueViolation, psycopg2.Error) as e:
                            director_skipped += 1
                            conn.rollback()
                            continue
            print(f"✓ Loaded {director_count} directors ({director_skipped} skipped due to errors)")
        except FileNotFoundError:
            print(f"✗ File not found: {director_file}")
        
        # Load ActsIn data from IMDBCast.txt
        actsin_file = os.path.join(data_dir, "IMDBCast.txt")
        print(f"\nLoading ActsIn data from {actsin_file}...")
        actsin_count = 0
        actsin_skipped = 0
        
        try:
            with open(actsin_file, 'r', encoding='latin-1') as f:
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) >= 3:
                        try:
                            pid = int(parts[0])
                            mid = int(parts[1])
                            role = parts[2]
                            
                            cur.execute(
                                "INSERT INTO ActsIn (pid, mid, role) VALUES (%s, %s, %s)",
                                (pid, mid, role)
                            )
                            conn.commit()
                            actsin_count += 1
                        except (ValueError, errors.UniqueViolation, errors.ForeignKeyViolation, psycopg2.Error) as e:
                            actsin_skipped += 1
                            conn.rollback()
                            continue
            print(f"✓ Loaded {actsin_count} acting records ({actsin_skipped} skipped due to errors)")
        except FileNotFoundError:
            print(f"✗ File not found: {actsin_file}")
        
        # Load Directs data from IMDBMovie_Directors.txt
        directs_file = os.path.join(data_dir, "IMDBMovie_Directors.txt")
        print(f"\nLoading Directs data from {directs_file}...")
        directs_count = 0
        directs_skipped = 0
        
        try:
            with open(directs_file, 'r', encoding='latin-1') as f:
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) >= 2:
                        try:
                            did = int(parts[0])
                            mid = int(parts[1])
                            
                            cur.execute(
                                "INSERT INTO Directs (did, mid) VALUES (%s, %s)",
                                (did, mid)
                            )
                            conn.commit()
                            directs_count += 1
                        except (ValueError, errors.UniqueViolation, errors.ForeignKeyViolation, psycopg2.Error) as e:
                            directs_skipped += 1
                            conn.rollback()
                            continue
            print(f"✓ Loaded {directs_count} directing records ({directs_skipped} skipped due to errors)")
        except FileNotFoundError:
            print(f"✗ File not found: {directs_file}")
        
        # Summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Movies loaded:        {movie_count}")
        print(f"Persons loaded:       {person_count}")
        print(f"Directors loaded:     {director_count}")
        print(f"ActsIn records:       {actsin_count}")
        print(f"Directs records:      {directs_count}")
        print("=" * 60)
        
        # Verify tables
        print("\nVerifying tables in moviesdb:")
        cur.execute("""
            SELECT tablename FROM pg_catalog.pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        tables = cur.fetchall()
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cur.fetchone()[0]
            print(f"  - {table[0]}: {count} rows")
        
    except psycopg2.Error as e:
        print(f"\nDatabase error: {e}")
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
    create_tables_and_load_data()