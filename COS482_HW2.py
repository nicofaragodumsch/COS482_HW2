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
        
        # ============================================================
        #   FAST RESET: TRUNCATE INSTEAD OF VERY SLOW DROP TABLE
        # ============================================================
        print("Preparing tables...")
        try:
            cur.execute("""
                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='actsin') THEN
                        TRUNCATE ActsIn RESTART IDENTITY CASCADE;
                    END IF;
                    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='directs') THEN
                        TRUNCATE Directs RESTART IDENTITY CASCADE;
                    END IF;
                    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='movie') THEN
                        TRUNCATE Movie RESTART IDENTITY CASCADE;
                    END IF;
                    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='person') THEN
                        TRUNCATE Person RESTART IDENTITY CASCADE;
                    END IF;
                    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='director') THEN
                        TRUNCATE Director RESTART IDENTITY CASCADE;
                    END IF;
                END $$;
            """)
            conn.commit()
            print("Tables truncated.\n")
        except Exception:
            conn.rollback()
            print("Warning: Could not truncate (tables may not exist yet). Continuing...\n")

        # ============================================================
        #   CREATE TABLES (SAFE, ONLY IF THEY DO NOT ALREADY EXIST)
        # ============================================================

        print("Creating Movie table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Movie(
                id INTEGER PRIMARY KEY,
                name TEXT,
                year INTEGER,
                rank REAL
            )
        """)
        conn.commit()
        print("Movie table created successfully")

        print("Creating Person table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Person(
                id INTEGER PRIMARY KEY,
                fname TEXT,
                lname TEXT,
                gender TEXT
            )
        """)
        conn.commit()
        print("Person table created successfully")

        print("Creating Director table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Director(
                id INTEGER PRIMARY KEY,
                fname TEXT,
                lname TEXT
            )
        """)
        conn.commit()
        print("Director table created successfully")

        print("Creating ActsIn table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ActsIn(
                pid INTEGER,
                mid INTEGER,
                role TEXT,
                PRIMARY KEY (pid, mid)
            )
        """)
        conn.commit()
        print("ActsIn table created successfully")

        print("Creating Directs table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Directs(
                did INTEGER,
                mid INTEGER,
                PRIMARY KEY (did, mid)
            )
        """)
        conn.commit()
        print("Directs table created successfully\n")

        # ============================================================
        #   LOAD DATA FROM FILES (UNCHANGED FROM YOUR ORIGINAL)
        # ============================================================

        print("=" * 60)
        print("Loading data from IMDB files...")
        print("=" * 60)
        
        # ------------------- load Movie data ------------------------
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
                                    # Insert one by one to identify failures
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
                
                # Insert remaining
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
            
            print(f"✓ Loaded {movie_count} movies ({movie_skipped} skipped)")
        except FileNotFoundError:
            print(f"✗ File not found: {movie_file}")
        
        # ------------------- load Person data ------------------------
        person_file = os.path.join(data_dir, "IMDBPerson.txt")
        print(f"\nLoading Person data from {person_file}...")
        person_count = 0
        person_skipped = 0
        
        try:
            with open(person_file, 'r', encoding='latin-1') as f:
                reader = csv.reader(f)
                next(reader)
                
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
            
            print(f"✓ Loaded {person_count} persons ({person_skipped} skipped)")
        except FileNotFoundError:
            print(f"✗ File not found: {person_file}")
        
        # ------------------- load Director data ------------------------
        director_file = os.path.join(data_dir, "IMDBDirectors.txt")
        print(f"\nLoading Director data from {director_file}...")
        director_count = 0
        director_skipped = 0

        try:
            with open(director_file, 'r', encoding='latin-1') as f:
                reader = csv.reader(f)
                next(reader)
                
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
            
            print(f"✓ Loaded {director_count} directors ({director_skipped} skipped)")
        except FileNotFoundError:
            print(f"✗ File not found: {director_file}")
        
        # ------------------- load ActsIn data ------------------------
        actsin_file = os.path.join(data_dir, "IMDBCast.txt")
        print(f"\nLoading ActsIn data from {actsin_file}...")
        print("(Loading without foreign key constraints for speed...)")
        actsin_count = 0
        actsin_skipped = 0
        
        try:
            with open(actsin_file, 'r', encoding='latin-1') as f:
                reader = csv.reader(f)
                next(reader)
                
                batch = []
                batch_size = 5000
                for row in reader:
                    if len(row) >= 2:
                        try:
                            pid = int(row[0])
                            mid = int(row[1])
                            role = row[2] if len(row) > 2 else ''
                            batch.append((pid, mid, role))
                            
                            if len(batch) >= batch_size:
                                try:
                                    cur.executemany(
                                        "INSERT INTO ActsIn (pid, mid, role) VALUES (%s, %s, %s)",
                                        batch
                                    )
                                    conn.commit()
                                    actsin_count += len(batch)
                                    batch = []
                                except psycopg2.Error:
                                    conn.rollback()
                                    actsin_skipped += len(batch)
                                    batch = []
                        except (ValueError, IndexError):
                            actsin_skipped += 1
                            continue
                
                # Remaining
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
                        actsin_skipped += len(batch)
            
            print(f"✓ Loaded {actsin_count} acting records ({actsin_skipped} skipped)")
            
            # Cleanup invalid foreign keys
            print("  Removing records with invalid person or movie IDs...")

            # Index on mid for speed
            try:
                cur.execute("CREATE INDEX IF NOT EXISTS idx_actsin_mid ON ActsIn(mid)")
                conn.commit()
            except psycopg2.Error:
                conn.rollback()

            total_deleted = 0
            batch_limit = 100000

            while True:
                cur.execute("""
                    WITH to_delete AS (
                        SELECT ctid FROM ActsIn a
                        WHERE NOT EXISTS (SELECT 1 FROM Person p WHERE p.id = a.pid)
                           OR NOT EXISTS (SELECT 1 FROM Movie  m WHERE m.id = a.mid)
                        LIMIT %s
                    )
                    DELETE FROM ActsIn WHERE ctid IN (SELECT ctid FROM to_delete)
                    RETURNING 1
                """, (batch_limit,))
                deleted = cur.rowcount
                conn.commit()
                if deleted == 0:
                    break
                total_deleted += deleted
                print(f"  Progress: removed {total_deleted} records...", end='\r')

            print(f"\n  Removed {total_deleted} invalid records")
            actsin_count -= total_deleted

        except FileNotFoundError:
            print(f"✗ File not found: {actsin_file}")

        # ------------------- load Directs data ------------------------
        directs_file = os.path.join(data_dir, "IMDBMovie_Directors.txt")
        print(f"\nLoading Directs data from {directs_file}...")
        directs_count = 0
        directs_skipped = 0
        
        try:
            with open(directs_file, 'r', encoding='latin-1') as f:
                reader = csv.reader(f)
                next(reader)
                
                batch = []
                batch_size = 5000
                for row in reader:
                    if len(row) >= 2:
                        try:
                            did = int(row[0])
                            mid = int(row[1])
                            batch.append((did, mid))
                            
                            if len(batch) >= batch_size:
                                try:
                                    cur.executemany(
                                        "INSERT INTO Directs (did, mid) VALUES (%s, %s)",
                                        batch
                                    )
                                    conn.commit()
                                    directs_count += len(batch)
                                    batch = []
                                except psycopg2.Error:
                                    conn.rollback()
                                    directs_skipped += len(batch)
                                    batch = []
                        except (ValueError, IndexError):
                            directs_skipped += 1
                            continue
                
                # Remaining
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
                        directs_skipped += len(batch)
            
            print(f"✓ Loaded {directs_count} directing records ({directs_skipped} skipped)")
            
            print("  Removing invalid foreign key records...")

            # Index on mid
            try:
                cur.execute("CREATE INDEX IF NOT EXISTS idx_directs_mid ON Directs(mid)")
                conn.commit()
            except psycopg2.Error:
                conn.rollback()

            total_deleted = 0
            batch_limit = 100000

            while True:
                cur.execute("""
                    WITH to_delete AS (
                        SELECT ctid FROM Directs d
                        WHERE NOT EXISTS (SELECT 1 FROM Director r WHERE r.id = d.did)
                           OR NOT EXISTS (SELECT 1 FROM Movie    m WHERE m.id = d.mid)
                        LIMIT %s
                    )
                    DELETE FROM Directs WHERE ctid IN (SELECT ctid FROM to_delete)
                    RETURNING 1
                """, (batch_limit,))
                deleted = cur.rowcount
                conn.commit()
                if deleted == 0:
                    break
                total_deleted += deleted
                print(f"  Progress: removed {total_deleted} records...", end='\r')

            print(f"\n  Removed {total_deleted} invalid records")
            directs_count -= total_deleted

        except FileNotFoundError:
            print(f"✗ File not found: {directs_file}")
        
        # ============================================================
        #   SUMMARY
        # ============================================================
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
