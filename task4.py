import psycopg2
import csv
import getpass

def find_best_movies_in_years(start_year, end_year, k, output_filename):
    """
    Find the best k movies in years from start_year to end_year,
    ordered by rank in descending order.
    
    Parameters:
    -----------
    start_year : int
        The starting year (inclusive)
    end_year : int
        The ending year (inclusive)
    k : int
        The number of top movies to retrieve
    output_filename : str
        The name of the CSV file to save results
        
    Returns:
    --------
    list of tuples
        The query results as a list of tuples (id, name, year, rank)
    """
    
    # Prompt user for database password
    print("PostgreSQL Database Connection")
    db_password = getpass.getpass("Enter PostgreSQL password for user 'postgres': ")
    
    results = []
    
    try:
        # Connect to the moviesdb database
        conn = psycopg2.connect(
            host="localhost",
            dbname="moviesdb",
            user="postgres",
            password=db_password
        )
        cur = conn.cursor()
        print(f"Successfully connected to moviesdb database\n")
        
        # Execute query to find best k movies in the year range
        print(f"Finding top {k} movies from {start_year} to {end_year}...")
        
        query = """
            SELECT id, name, year, rank
            FROM Movie
            WHERE year >= %s AND year <= %s
              AND rank IS NOT NULL
              AND rank >= 0 AND rank <= 10
            ORDER BY rank DESC
            LIMIT %s
        """
        
        cur.execute(query, (start_year, end_year, k))
        results = cur.fetchall()
        
        print(f"Found {len(results)} movies")
        
        # Save results to CSV file with semicolon delimiter
        print(f"Saving results to {output_filename}...")
        
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=';')
            
            # Write header
            csv_writer.writerow(['id', 'name', 'year', 'rank'])
            
            # Write data rows
            for row in results:
                csv_writer.writerow(row)
        
        print(f"✓ Results saved to {output_filename}")
        
        # Display first few results
        print(f"\nTop 10 results:")
        print(f"{'ID':<10} {'Name':<50} {'Year':<10} {'Rank':<10}")
        print("-" * 80)
        for i, row in enumerate(results[:10]):
            print(f"{row[0]:<10} {row[1][:48]:<50} {row[2]:<10} {row[3]:<10.2f}")
        
        if len(results) > 10:
            print(f"... and {len(results) - 10} more rows")
        
    except psycopg2.Error as e:
        print(f"\nDatabase error: {e}")
        if conn:
            conn.rollback()
    
    finally:
        # Close cursor and connection
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
        print("\nDatabase connection closed")
    
    return results


def main():
    """
    Main function to demonstrate the find_best_movies_in_years function.
    Part (b): Find top 20 movies from 1995 to 2004
    """
    
    print("=" * 80)
    print("Task 4(b): Finding top 20 movies from 1995 to 2004")
    print("=" * 80)
    print()
    
    # Call the function with parameters for part (b)
    results = find_best_movies_in_years(
        start_year=1995,
        end_year=2004,
        k=20,
        output_filename='best_movies_1995_2004.csv'
    )
    
    print()
    print("=" * 80)
    print(f"Task completed! Found {len(results)} movies.")
    print("Output saved to: best_movies_1995_2004.csv")
    print("=" * 80)


if __name__ == "__main__":
    main()