import psycopg2
import getpass

def execute_queries():
    """
    Execute all SQL queries from Task 3 and save results to query_results.txt
    """
    
    # Prompt user for database password
    print("PostgreSQL Database Connection")
    db_password = getpass.getpass("Enter PostgreSQL password for user 'postgres': ")
    
    # Connect to the moviesdb database
    try:
        conn = psycopg2.connect(
            host="localhost",
            #dbname="moviesdb", uncomment this before submission
            dbname="moviesdb3",#for testing purposes, delete before submission
            user="postgres",
            password=db_password
        )
        cur = conn.cursor()
        print("Successfully connected to moviesdb database\n")
        
        # Open output file
        with open('query_results.txt', 'w', encoding='utf-8') as f:
            
            # Query (a)
            print("Executing query (a)...")
            f.write("=" * 80 + "\n")
            f.write("(a) Persons who acted in both second half of 19th and first half of 20th century\n")
            f.write("=" * 80 + "\n")
            
            cur.execute("""
                SELECT DISTINCT p.id, p.fname, p.lname, p.gender
                FROM Person p
                WHERE EXISTS (
                    SELECT 1 
                    FROM ActsIn a1
                    JOIN Movie m1 ON a1.mid = m1.id
                    WHERE a1.pid = p.id 
                      AND m1.year >= 1850 
                      AND m1.year < 1900
                )
                AND EXISTS (
                    SELECT 1 
                    FROM ActsIn a2
                    JOIN Movie m2 ON a2.mid = m2.id
                    WHERE a2.pid = p.id 
                      AND m2.year >= 1900 
                      AND m2.year < 1950
                )
                LIMIT 10
            """)
            results = cur.fetchall()
            f.write(f"{'ID':<10} {'First Name':<20} {'Last Name':<20} {'Gender':<10}\n")
            f.write("-" * 80 + "\n")
            for row in results:
                f.write(f"{row[0]:<10} {row[1]:<20} {row[2]:<20} {row[3]:<10}\n")
            f.write(f"\nTotal rows: {len(results)}\n\n")
            
            # Query (b)
            print("Executing query (b)...")
            f.write("=" * 80 + "\n")
            f.write("(b) Directors who directed a film in a leap year\n")
            f.write("=" * 80 + "\n")
            
            cur.execute("""
                SELECT DISTINCT d.id, d.fname, d.lname
                FROM Director d
                JOIN Directs dr ON d.id = dr.did
                JOIN Movie m ON dr.mid = m.id
                WHERE m.year IS NOT NULL
                  AND (m.year % 4 = 0 AND (m.year % 100 != 0 OR m.year % 400 = 0))
                LIMIT 10
            """)
            results = cur.fetchall()
            f.write(f"{'ID':<10} {'First Name':<20} {'Last Name':<20}\n")
            f.write("-" * 80 + "\n")
            for row in results:
                f.write(f"{row[0]:<10} {row[1]:<20} {row[2]:<20}\n")
            f.write(f"\nTotal rows: {len(results)}\n\n")
            
            # Query (c)
            print("Executing query (c)...")
            f.write("=" * 80 + "\n")
            f.write("(c) Top 10 movies with same year as 'Shrek (2001)' but better rank\n")
            f.write("=" * 80 + "\n")
            
            cur.execute("""
                SELECT m.id, m.name, m.year, m.rank
                FROM Movie m
                WHERE m.year = (SELECT year FROM Movie WHERE name = 'Shrek (2001)')
                  AND m.rank > (SELECT rank FROM Movie WHERE name = 'Shrek (2001)')
                ORDER BY m.rank DESC
                LIMIT 10
            """)
            results = cur.fetchall()
            f.write(f"{'ID':<10} {'Name':<50} {'Year':<10} {'Rank':<10}\n")
            f.write("-" * 80 + "\n")
            for row in results:
                f.write(f"{row[0]:<10} {row[1]:<50} {row[2]:<10} {row[3]:<10.2f}\n")
            f.write(f"\nTotal rows: {len(results)}\n\n")
            
            # Query (d)
            print("Executing query (d)...")
            f.write("=" * 80 + "\n")
            f.write("(d) Top 10 directors by number of films directed\n")
            f.write("=" * 80 + "\n")
            
            cur.execute("""
                SELECT d.id, d.fname, d.lname, COUNT(dr.mid) AS num_films
                FROM Director d
                JOIN Directs dr ON d.id = dr.did
                GROUP BY d.id, d.fname, d.lname
                ORDER BY num_films DESC
                LIMIT 10
            """)
            results = cur.fetchall()
            f.write(f"{'ID':<10} {'First Name':<20} {'Last Name':<20} {'# Films':<10}\n")
            f.write("-" * 80 + "\n")
            for row in results:
                f.write(f"{row[0]:<10} {row[1]:<20} {row[2]:<20} {row[3]:<10}\n")
            f.write(f"\nTotal rows: {len(results)}\n\n")
            
            # Query (e) - Largest
            print("Executing query (e) - largest...")
            f.write("=" * 80 + "\n")
            f.write("(e) Movies with LARGEST number of actors\n")
            f.write("=" * 80 + "\n")
            
            cur.execute("""
                WITH ActorCounts AS (
                    SELECT m.id, m.name, m.year, COUNT(a.pid) AS num_actors
                    FROM Movie m
                    JOIN ActsIn a ON m.id = a.mid
                    GROUP BY m.id, m.name, m.year
                )
                SELECT id, name, year, num_actors
                FROM ActorCounts
                WHERE num_actors = (SELECT MAX(num_actors) FROM ActorCounts)
                ORDER BY id
            """)
            results = cur.fetchall()
            f.write(f"{'ID':<10} {'Name':<50} {'Year':<10} {'# Actors':<10}\n")
            f.write("-" * 80 + "\n")
            for row in results:
                f.write(f"{row[0]:<10} {row[1]:<50} {row[2]:<10} {row[3]:<10}\n")
            f.write(f"\nTotal rows: {len(results)}\n\n")
            
            # Query (e) - Smallest
            print("Executing query (e) - smallest...")
            f.write("=" * 80 + "\n")
            f.write("(e) Movies with SMALLEST number of actors\n")
            f.write("=" * 80 + "\n")
            
            cur.execute("""
                WITH ActorCounts AS (
                    SELECT m.id, m.name, m.year, COUNT(a.pid) AS num_actors
                    FROM Movie m
                    JOIN ActsIn a ON m.id = a.mid
                    GROUP BY m.id, m.name, m.year
                )
                SELECT id, name, year, num_actors
                FROM ActorCounts
                WHERE num_actors = (SELECT MIN(num_actors) FROM ActorCounts)
                ORDER BY id
                LIMIT 10
            """)
            results = cur.fetchall()
            f.write(f"{'ID':<10} {'Name':<50} {'Year':<10} {'# Actors':<10}\n")
            f.write("-" * 80 + "\n")
            for row in results:
                f.write(f"{row[0]:<10} {row[1]:<50} {row[2]:<10} {row[3]:<10}\n")
            f.write(f"\nTotal rows: {len(results)}\n\n")
            
            # Query (f)
            print("Executing query (f)...")
            f.write("=" * 80 + "\n")
            f.write("(f) Actors who worked with at least 10 distinct directors\n")
            f.write("=" * 80 + "\n")
            
            cur.execute("""
                SELECT p.id, p.fname, p.lname, COUNT(DISTINCT dr.did) AS num_directors
                FROM Person p
                JOIN ActsIn a ON p.id = a.pid
                JOIN Directs dr ON a.mid = dr.mid
                GROUP BY p.id, p.fname, p.lname
                HAVING COUNT(DISTINCT dr.did) >= 10
                ORDER BY num_directors DESC
                LIMIT 10
            """)
            results = cur.fetchall()
            f.write(f"{'ID':<10} {'First Name':<20} {'Last Name':<20} {'# Directors':<12}\n")
            f.write("-" * 80 + "\n")
            for row in results:
                f.write(f"{row[0]:<10} {row[1]:<20} {row[2]:<20} {row[3]:<12}\n")
            f.write(f"\nTotal rows: {len(results)}\n\n")
        
        print("\n✓ All queries executed successfully!")
        print("✓ Results saved to query_results.txt")
        
        # Also create sql.txt with just the queries
        with open('sql.txt', 'w', encoding='utf-8') as f:
            f.write("-- SQL Queries for Task 3\n\n")
            
            f.write("-- (a) Persons who acted in both second half of 19th and first half of 20th century\n")
            f.write("""SELECT DISTINCT p.id, p.fname, p.lname, p.gender
FROM Person p
WHERE EXISTS (
    SELECT 1 
    FROM ActsIn a1
    JOIN Movie m1 ON a1.mid = m1.id
    WHERE a1.pid = p.id 
      AND m1.year >= 1850 
      AND m1.year < 1900
)
AND EXISTS (
    SELECT 1 
    FROM ActsIn a2
    JOIN Movie m2 ON a2.mid = m2.id
    WHERE a2.pid = p.id 
      AND m2.year >= 1900 
      AND m2.year < 1950
)
LIMIT 10;

""")
            
            f.write("-- (b) Directors who directed a film in a leap year\n")
            f.write("""SELECT DISTINCT d.id, d.fname, d.lname
FROM Director d
JOIN Directs dr ON d.id = dr.did
JOIN Movie m ON dr.mid = m.id
WHERE m.year IS NOT NULL
  AND (m.year % 4 = 0 AND (m.year % 100 != 0 OR m.year % 400 = 0))
LIMIT 10;

""")
            
            f.write("-- (c) Top 10 movies with same year as 'Shrek (2001)' but better rank\n")
            f.write("""SELECT m.id, m.name, m.year, m.rank
FROM Movie m
WHERE m.year = (SELECT year FROM Movie WHERE name = 'Shrek (2001)')
  AND m.rank > (SELECT rank FROM Movie WHERE name = 'Shrek (2001)')
ORDER BY m.rank DESC
LIMIT 10;

""")
            
            f.write("-- (d) Top 10 directors by number of films directed\n")
            f.write("""SELECT d.id, d.fname, d.lname, COUNT(dr.mid) AS num_films
FROM Director d
JOIN Directs dr ON d.id = dr.did
GROUP BY d.id, d.fname, d.lname
ORDER BY num_films DESC
LIMIT 10;

""")
            
            f.write("-- (e) Movies with LARGEST number of actors\n")
            f.write("""WITH ActorCounts AS (
    SELECT m.id, m.name, m.year, COUNT(a.pid) AS num_actors
    FROM Movie m
    JOIN ActsIn a ON m.id = a.mid
    GROUP BY m.id, m.name, m.year
)
SELECT id, name, year, num_actors
FROM ActorCounts
WHERE num_actors = (SELECT MAX(num_actors) FROM ActorCounts)
ORDER BY id;

""")
            
            f.write("-- (e) Movies with SMALLEST number of actors\n")
            f.write("""WITH ActorCounts AS (
    SELECT m.id, m.name, m.year, COUNT(a.pid) AS num_actors
    FROM Movie m
    JOIN ActsIn a ON m.id = a.mid
    GROUP BY m.id, m.name, m.year
)
SELECT id, name, year, num_actors
FROM ActorCounts
WHERE num_actors = (SELECT MIN(num_actors) FROM ActorCounts)
ORDER BY id
LIMIT 10;

""")
            
            f.write("-- (f) Actors who worked with at least 10 distinct directors\n")
            f.write("""SELECT p.id, p.fname, p.lname, COUNT(DISTINCT dr.did) AS num_directors
FROM Person p
JOIN ActsIn a ON p.id = a.pid
JOIN Directs dr ON a.mid = dr.mid
GROUP BY p.id, p.fname, p.lname
HAVING COUNT(DISTINCT dr.did) >= 10
ORDER BY num_directors DESC
LIMIT 10;
""")
        
        print("✓ SQL queries saved to sql.txt")
        
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
    execute_queries()