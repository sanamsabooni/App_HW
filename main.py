import fetch_data

def main():
    """Main script to fetch and store data."""
    print("Starting data fetch process...")
    
    try:
        fetch_data.fetch_and_store_data()
        print("Data fetching and storage completed successfully.")
    except Exception as e:
        print(f"Error during execution: {e}")

if __name__ == "__main__":
    main()
