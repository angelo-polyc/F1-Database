import sys
from db.setup import setup_database
from sources import get_source
import query_data

def run_pull(source_name: str):
    print(f"Starting pull from: {source_name}")
    source = get_source(source_name)
    records = source.pull()
    print(f"Pull complete. Total records: {records}")

def run_query():
    print("\n=== Data Query Interface ===")
    print("1. Get latest values for a metric")
    print("2. Get time series for asset/metric")
    print("3. List available metrics")
    print("4. List available assets")
    print("5. View pull history")
    print("6. Exit")
    
    while True:
        choice = input("\nSelect option (1-6): ").strip()
        
        if choice == "1":
            metric = input("Enter metric name: ").strip()
            source = input("Filter by source (or press Enter for all): ").strip() or None
            results = query_data.get_latest_values(metric, source)
            
            if results:
                for r in results[:20]:
                    print(f"  {r['asset']}: {r['value']} ({r['pulled_at']})")
                if len(results) > 20:
                    print(f"  ... and {len(results) - 20} more")
                
                export = input("Export to CSV? (y/n): ").strip().lower()
                if export == "y":
                    filename = input("Filename (default: export.csv): ").strip() or "export.csv"
                    query_data.export_to_csv(results, filename)
            else:
                print("No data found")
        
        elif choice == "2":
            asset = input("Enter asset: ").strip()
            metric = input("Enter metric name: ").strip()
            source = input("Filter by source (or press Enter for all): ").strip() or None
            results = query_data.get_time_series(asset, metric, source)
            
            if results:
                for r in results[:20]:
                    print(f"  {r['pulled_at']}: {r['value']}")
                if len(results) > 20:
                    print(f"  ... and {len(results) - 20} more")
                
                export = input("Export to CSV? (y/n): ").strip().lower()
                if export == "y":
                    filename = input("Filename (default: export.csv): ").strip() or "export.csv"
                    query_data.export_to_csv(results, filename)
            else:
                print("No data found")
        
        elif choice == "3":
            metrics = query_data.list_available_metrics()
            if metrics:
                print("Available metrics:")
                for m in metrics:
                    print(f"  - {m}")
            else:
                print("No metrics found")
        
        elif choice == "4":
            metric = input("Filter by metric (or press Enter for all): ").strip() or None
            assets = query_data.list_available_assets(metric)
            if assets:
                print(f"Available assets ({len(assets)}):")
                for a in assets[:50]:
                    print(f"  - {a}")
                if len(assets) > 50:
                    print(f"  ... and {len(assets) - 50} more")
            else:
                print("No assets found")
        
        elif choice == "5":
            source = input("Filter by source (or press Enter for all): ").strip() or None
            history = query_data.get_pull_history(source)
            if history:
                print("\nPull History:")
                for h in history:
                    print(f"  [{h['pull_id']}] {h['source_name']} - {h['pulled_at']} - {h['status']} ({h['records_count']} records)")
            else:
                print("No pull history found")
        
        elif choice == "6":
            print("Exiting...")
            break
        
        else:
            print("Invalid option")

def print_usage():
    print("Usage:")
    print("  python main.py setup              - Initialize database tables")
    print("  python main.py pull <source>      - Pull data from a source (e.g., artemis)")
    print("  python main.py query              - Interactive query interface")
    print("  python main.py sources            - List available sources")

def main():
    if len(sys.argv) < 2:
        print_usage()
        return
    
    command = sys.argv[1].lower()
    
    if command == "setup":
        setup_database()
    
    elif command == "pull":
        if len(sys.argv) < 3:
            print("Error: Please specify a source name")
            print("Example: python main.py pull artemis")
            return
        source_name = sys.argv[2].lower()
        run_pull(source_name)
    
    elif command == "query":
        run_query()
    
    elif command == "sources":
        from sources import SOURCES
        print("Available sources:")
        for name in SOURCES:
            print(f"  - {name}")
    
    else:
        print(f"Unknown command: {command}")
        print_usage()

if __name__ == "__main__":
    main()
