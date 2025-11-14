import psycopg2
from psycopg2 import errors
import getpass
import os
import csv

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
                PRIMARY KEY (pid, mid)
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
                PRIMARY KEY (did, mid)
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
                reader = csv.reader(f)
                next(reader)  # Skip header
                
                batch = []
                for row in reader:
                    if len(row) >= 4:
                        try:
                            movie_id = int(row[0])
                            name = row[1]
                            year = int(row[2]) if row[2] else None
                            rank = float(row[3]) if row[3] else None
                            batch.append((movie_id, name, year, rank))
                            
                            # Batch insert every 1000 rows
                            if len(batch) >= 1000:
                                try:
                                    cur.executemany(
                                        "INSERT INTO Movie (id, name, year, rank) VALUES (%s, %s, %s, %s)",
                                        batch
                                    )
                                    conn.commit()
                                    movie_count += len(batch)
                                    batch = []
                                except psycopg2.Error:
                                    conn.rollback()
                                    # Insert one by one to identify which rows fail
                                    for item in batch:
                                        try:
                                            cur.execute(
                                                "INSERT INTO Movie (id, name, year, rank) VALUES (%s, %s, %s, %s)",
                                                item
                                            )
                                            conn.commit()
                                            movie_count += 1
                                        except psycopg2.Error:
                                            conn.rollback()
                                            movie_skipped += 1
                                    batch = []
                        except (ValueError, IndexError):
                            movie_skipped += 1
                            continue
                
                # Insert remaining batch
                if batch:
                    try:
                        cur.executemany(
                            "INSERT INTO Movie (id, name, year, rank) VALUES (%s, %s, %s, %s)",
                            batch
                        )
                        conn.commit()
                        movie_count += len(batch)
                    except psycopg2.Error:
                        conn.rollback()
                        for item in batch:
                            try:
                                cur.execute(
                                    "INSERT INTO Movie (id, name, year, rank) VALUES (%s, %s, %s, %s)",
                                    item
                                )
                                conn.commit()
                                movie_count += 1
                            except psycopg2.Error:
                                conn.rollback()
                                movie_skipped += 1
                
            print(f"✓ Loaded {movie_count} movies ({movie_skipped} skipped due to errors)")
            if movie_skipped > 0:
                print(f"  (Note: Skipped movies are likely duplicates with same ID)")
        except FileNotFoundError:
            print(f"✗ File not found: {movie_file}")
        
        # Load Person data from IMDBPerson.txt
        person_file = os.path.join(data_dir, "IMDBPerson.txt")
        print(f"\nLoading Person data from {person_file}...")
        person_count = 0
        person_skipped = 0
        
        try:
            with open(person_file, 'r', encoding='latin-1') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                
                batch = []
                for row in reader:
                    if len(row) >= 4:
                        try:
                            person_id = int(row[0])
                            fname = row[1]
                            lname = row[2]
                            gender = row[3]
                            batch.append((person_id, fname, lname, gender))
                            
                            if len(batch) >= 1000:
                                try:
                                    cur.executemany(
                                        "INSERT INTO Person (id, fname, lname, gender) VALUES (%s, %s, %s, %s)",
                                        batch
                                    )
                                    conn.commit()
                                    person_count += len(batch)
                                    batch = []
                                except psycopg2.Error:
                                    conn.rollback()
                                    for item in batch:
                                        try:
                                            cur.execute(
                                                "INSERT INTO Person (id, fname, lname, gender) VALUES (%s, %s, %s, %s)",
                                                item
                                            )
                                            conn.commit()
                                            person_count += 1
                                        except psycopg2.Error:
                                            conn.rollback()
                                            person_skipped += 1
                                    batch = []
                        except (ValueError, IndexError):
                            person_skipped += 1
                            continue
                
                if batch:
                    try:
                        cur.executemany(
                            "INSERT INTO Person (id, fname, lname, gender) VALUES (%s, %s, %s, %s)",
                            batch
                        )
                        conn.commit()
                        person_count += len(batch)
                    except psycopg2.Error:
                        conn.rollback()
                        for item in batch:
                            try:
                                cur.execute(
                                    "INSERT INTO Person (id, fname, lname, gender) VALUES (%s, %s, %s, %s)",
                                    item
                                )
                                conn.commit()
                                person_count += 1
                            except psycopg2.Error:
                                conn.rollback()
                                person_skipped += 1
                
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
                reader = csv.reader(f)
                next(reader)  # Skip header
                
                batch = []
                for row in reader:
                    if len(row) >= 3:
                        try:
                            director_id = int(row[0])
                            fname = row[1]
                            lname = row[2]
                            batch.append((director_id, fname, lname))
                            
                            if len(batch) >= 1000:
                                try:
                                    cur.executemany(
                                        "INSERT INTO Director (id, fname, lname) VALUES (%s, %s, %s)",
                                        batch
                                    )
                                    conn.commit()
                                    director_count += len(batch)
                                    batch = []
                                except psycopg2.Error:
                                    conn.rollback()
                                    for item in batch:
                                        try:
                                            cur.execute(
                                                "INSERT INTO Director (id, fname, lname) VALUES (%s, %s, %s)",
                                                item
                                            )
                                            conn.commit()
                                            director_count += 1
                                        except psycopg2.Error:
                                            conn.rollback()
                                            director_skipped += 1
                                    batch = []
                        except (ValueError, IndexError):
                            director_skipped += 1
                            continue
                
                if batch:
                    try:
                        cur.executemany(
                            "INSERT INTO Director (id, fname, lname) VALUES (%s, %s, %s)",
                            batch
                        )
                        conn.commit()
                        director_count += len(batch)
                    except psycopg2.Error:
                        conn.rollback()
                        for item in batch:
                            try:
                                cur.execute(
                                    "INSERT INTO Director (id, fname, lname) VALUES (%s, %s, %s)",
                                    item
                                )
                                conn.commit()
                                director_count += 1
                            except psycopg2.Error:
                                conn.rollback()
                                director_skipped += 1
                
            print(f"✓ Loaded {director_count} directors ({director_skipped} skipped due to errors)")
        except FileNotFoundError:
            print(f"✗ File not found: {director_file}")
        
        # Load ActsIn data from IMDBCast.txt
        actsin_file = os.path.join(data_dir, "IMDBCast.txt")
        print(f"\nLoading ActsIn data from {actsin_file}...")
        print("(Loading without foreign key constraints for speed...)")
        actsin_count = 0
        actsin_skipped = 0
        
        try:
            with open(actsin_file, 'r', encoding='latin-1') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                
                batch = []
                batch_size = 5000  # Larger batch size for speed
                seen_pairs = set()  # Track (pid, mid) pairs to avoid duplicates
                
                for row in reader:
                    if len(row) >= 2:  # Only need pid and mid
                        try:
                            pid = int(row[0])
                            mid = int(row[1])
                            role = row[2] if len(row) > 2 else ''
                            
                            # Skip if we've already seen this (pid, mid) pair
                            if (pid, mid) in seen_pairs:
                                actsin_skipped += 1
                                continue
                            
                            seen_pairs.add((pid, mid))
                            batch.append((pid, mid, role))
                            
                            if len(batch) >= batch_size:
                                try:
                                    cur.executemany(
                                        "INSERT INTO ActsIn (pid, mid, role) VALUES (%s, %s, %s)",
                                        batch
                                    )
                                    conn.commit()
                                    actsin_count += len(batch)
                                    print(f"  Progress: {actsin_count} records loaded...", end='\r')
                                    batch = []
                                except psycopg2.Error as e:
                                    conn.rollback()
                                    # Try one by one if batch fails
                                    for item in batch:
                                        try:
                                            cur.execute(
                                                "INSERT INTO ActsIn (pid, mid, role) VALUES (%s, %s, %s)",
                                                item
                                            )
                                            conn.commit()
                                            actsin_count += 1
                                        except psycopg2.Error:
                                            conn.rollback()
                                            actsin_skipped += 1
                                    batch = []
                        except (ValueError, IndexError):
                            actsin_skipped += 1
                            continue
                
                if batch:
                    try:
                        cur.executemany(
                            "INSERT INTO ActsIn (pid, mid, role) VALUES (%s, %s, %s)",
                            batch
                        )
                        conn.commit()
                        actsin_count += len(batch)
                    except psycopg2.Error:
                        conn.rollback()
                        for item in batch:
                            try:
                                cur.execute(
                                    "INSERT INTO ActsIn (pid, mid, role) VALUES (%s, %s, %s)",
                                    item
                                )
                                conn.commit()
                                actsin_count += 1
                            except psycopg2.Error:
                                conn.rollback()
                                actsin_skipped += 1
                
            print(f"\n✓ Loaded {actsin_count} acting records ({actsin_skipped} skipped)")
            
            # Now delete rows that violate foreign key constraints
            print("  Removing records with invalid person or movie IDs...")
            print("  (Creating indexes to speed up deletion...)")
            
            # Create indexes on foreign key columns for faster lookups
            cur.execute("CREATE INDEX IF NOT EXISTS idx_actsin_pid ON ActsIn(pid)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_actsin_mid ON ActsIn(mid)")
            conn.commit()
            
            # Use NOT EXISTS which is faster than NOT IN for large datasets
            cur.execute("""
                DELETE FROM ActsIn a
                WHERE NOT EXISTS (SELECT 1 FROM Person p WHERE p.id = a.pid)
                   OR NOT EXISTS (SELECT 1 FROM Movie m WHERE m.id = a.mid)
            """)
            deleted = cur.rowcount
            conn.commit()
            print(f"  Removed {deleted} records with invalid foreign keys")
            actsin_count -= deleted
            
        except FileNotFoundError:
            print(f"✗ File not found: {actsin_file}")
        
        # Load Directs data from IMDBMovie_Directors.txt
        directs_file = os.path.join(data_dir, "IMDBMovie_Directors.txt")
        print(f"\nLoading Directs data from {directs_file}...")
        directs_count = 0
        directs_skipped = 0
        
        try:
            with open(directs_file, 'r', encoding='latin-1') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                
                batch = []
                batch_size = 5000
                seen_pairs = set()  # Track (did, mid) pairs to avoid duplicates
                
                for row in reader:
                    if len(row) >= 2:
                        try:
                            did = int(row[0])
                            mid = int(row[1])
                            
                            # Skip if we've already seen this (did, mid) pair
                            if (did, mid) in seen_pairs:
                                directs_skipped += 1
                                continue
                            
                            seen_pairs.add((did, mid))
                            batch.append((did, mid))
                            
                            if len(batch) >= batch_size:
                                try:
                                    cur.executemany(
                                        "INSERT INTO Directs (did, mid) VALUES (%s, %s)",
                                        batch
                                    )
                                    conn.commit()
                                    directs_count += len(batch)
                                    print(f"  Progress: {directs_count} records loaded...", end='\r')
                                    batch = []
                                except psycopg2.Error as e:
                                    conn.rollback()
                                    # Try one by one if batch fails
                                    for item in batch:
                                        try:
                                            cur.execute(
                                                "INSERT INTO Directs (did, mid) VALUES (%s, %s)",
                                                item
                                            )
                                            conn.commit()
                                            directs_count += 1
                                        except psycopg2.Error:
                                            conn.rollback()
                                            directs_skipped += 1
                                    batch = []
                        except (ValueError, IndexError):
                            directs_skipped += 1
                            continue
                
                if batch:
                    try:
                        cur.executemany(
                            "INSERT INTO Directs (did, mid) VALUES (%s, %s)",
                            batch
                        )
                        conn.commit()
                        directs_count += len(batch)
                    except psycopg2.Error:
                        conn.rollback()
                        for item in batch:
                            try:
                                cur.execute(
                                    "INSERT INTO Directs (did, mid) VALUES (%s, %s)",
                                    item
                                )
                                conn.commit()
                                directs_count += 1
                            except psycopg2.Error:
                                conn.rollback()
                                directs_skipped += 1
                
            print(f"\n✓ Loaded {directs_count} directing records ({directs_skipped} skipped)")
            
            # Remove records with invalid foreign keys
            print("  Removing records with invalid director or movie IDs...")
            print("  (Creating indexes to speed up deletion...)")
            
            # Create indexes for faster lookups
            cur.execute("CREATE INDEX IF NOT EXISTS idx_directs_did ON Directs(did)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_directs_mid ON Directs(mid)")
            conn.commit()
            
            # Use NOT EXISTS which is faster than NOT IN
            cur.execute("""
                DELETE FROM Directs d
                WHERE NOT EXISTS (SELECT 1 FROM Director r WHERE r.id = d.did)
                   OR NOT EXISTS (SELECT 1 FROM Movie m WHERE m.id = d.mid)
            """)
            deleted = cur.rowcount
            conn.commit()
            print(f"  Removed {deleted} records with invalid foreign keys")
            directs_count -= deleted
            
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