#!/usr/bin/env python3
# /// script
# dependencies = [
#   "pyyaml",
#   "requests",
#   "duckdb",
# ]
# ///
import os
import sys
import subprocess
from pathlib import Path
import yaml
import json
import argparse
import importlib.util

def load_module_from_path(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def clear_screen():
    subprocess.run(['clear'] if os.name != 'nt' else ['cls'], check=False)

def print_header(text):
    print("\n" + "="*60)
    print(f"  {text}")
    print("="*60 + "\n")

def clean_path(path_str):
    if not path_str:
        return ""
    # Remove outer quotes
    path_str = path_str.strip().strip("'").strip('"')
    
    # Handle common shell escapes: replace \ followed by space or ~ or other chars
    import re
    path_str = re.sub(r'\\(.)', r'\1', path_str)
    
    # Expand home directory ~
    return os.path.expanduser(path_str)

def get_input(prompt, default=None):
    if default:
        res = input(f"{prompt} [{default}]: ").strip()
        return clean_path(res if res else default)
    return clean_path(input(f"{prompt}: ").strip())

def run_cmd(cmd, cwd=None, env=None):
    # Standard uv run within project context
    uv_cmd = ["uv", "run"] + cmd
    
    print(f"Running: {' '.join(uv_cmd)}")
    result = subprocess.run(uv_cmd, cwd=cwd, env=env)
    return result.returncode == 0

def main():
    parser = argparse.ArgumentParser(description="Personal Data Reflection Setup & Launcher")
    parser.add_argument("--serve", action="store_true", help="Skip setup and start the web UI immediately")
    parser.add_argument("--reset-strava", action="store_true", help="Clear all Strava data from the database")
    parser.add_argument("--reset-health", action="store_true", help="Clear all Apple Health data from the database")
    parser.add_argument("--demo", action="store_true", help="Run with generated sample data")
    args = parser.parse_args()

    clear_screen()
    print_header("Personal Data Reflection Setup")
    
    # Define Directories
    root_dir = Path(__file__).parent.parent.absolute()
    reflection_dir = root_dir / "personal-data-reflection"
    strava_dir = root_dir / "strava-data-puller"
    health_dir = root_dir / "apple-health-export"

    # Load Configuration
    config_file = reflection_dir / ".setup_config.json"
    setup_config = {}
    if config_file.exists():
        with open(config_file, "r") as f:
            setup_config = json.load(f)


    # Handle reset flags
    db_path_val = setup_config.get("db_path")
    if args.reset_strava or args.reset_health:
        if not db_path_val:
            print("Error: No database configured. Run setup first.")
            sys.exit(1)
        
        db_path = Path(db_path_val).absolute()
        if not db_path.exists():
            print(f"Error: Database not found at {db_path}")
            sys.exit(1)
        
        import duckdb
        con = duckdb.connect(str(db_path))
        
        if args.reset_strava:
            print("Clearing Strava data...")
            con.execute("DELETE FROM strava_activities")
            con.execute("DELETE FROM workouts WHERE source = 'strava'")
            print("Strava data cleared.")
        
        if args.reset_health:
            print("Clearing Apple Health data...")
            con.execute("DELETE FROM health_metrics")
            con.execute("DELETE FROM workouts WHERE source = 'apple_health'")
            con.execute("DELETE FROM daily_summary")
            print("Apple Health data cleared.")
        
        con.close()
        print("\nDone! You can now re-import data.")
        return

    # Demo Mode
    if args.demo:
        print_header("Running in Demo Mode")
        
        # Use a demo specific database to valid interfering with real setup
        demo_db = reflection_dir / "data" / "demo_reflection.duckdb"
        print(f"Using demo database: {demo_db}")
        
        # dynamic import of generate_sample_data
        gen_script = reflection_dir / "generate_sample_data.py"
        if not gen_script.exists():
             print("Error: generate_sample_data.py not found.")
             sys.exit(1)
             
        print("Generating sample data...")
        # Imported programmatically to avoid top-level import errors if file missing
        gen_module = load_module_from_path("generate_sample_data", gen_script)
        gen_module.generate_sample_data(str(demo_db))
        
        print("\nStarting Dashboard with Sample Data...")
        try:
            run_cmd(["python3", "reflect.py", "--database", str(demo_db), "serve", "--port", "5001", "--debug"], 
                    cwd=reflection_dir)
        except KeyboardInterrupt:
            print("\nShutting down.")
        return

    # Fast Path for --serve
    if args.serve and db_path_val:
        db_path = Path(db_path_val).absolute()
        print(f"Launching dashboard with database: {db_path}")
        try:
            run_cmd(["python3", "reflect.py", "--database", str(db_path), "serve", "--port", "5001", "--debug"], 
                    cwd=reflection_dir)
        except KeyboardInterrupt:
            print("\nShutting down.")
        return

    db_path = get_input("Enter path for DuckDB database", setup_config.get("db_path", str(reflection_dir / "data" / "reflection.duckdb")))
    db_path = Path(db_path).absolute()
    
    # If user provided a directory, append the filename
    if db_path.is_dir():
        db_path = db_path / "reflection.duckdb"
    
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        # On macOS, mkdir -p can fail on iCloud root folders even if they exist
        if not db_path.parent.exists():
            print(f"Error: No permission to create directory {db_path.parent}")
            sys.exit(1)
    
    # Save current DB path to config
    setup_config["db_path"] = str(db_path)
    with open(config_file, "w") as f:
        json.dump(setup_config, f)

    # 2. Strava Secrets
    print_header("Strava API Configuration")
    print("Get these from https://www.strava.com/settings/api")
    
    # Load existing secrets if they exist
    env_file = strava_dir / ".env"
    existing_secrets = {}
    if env_file.exists():
        with open(env_file, "r") as f:
            for line in f:
                if "=" in line:
                    key, val = line.strip().split("=", 1)
                    existing_secrets[key] = val
                    
    use_strava = get_input("Setup Strava import? (y/n)", "y").lower() == 'y'
    
    st_env = os.environ.copy()
    if use_strava:
        st_client_id = get_input("STRAVA_CLIENT_ID", existing_secrets.get("STRAVA_CLIENT_ID"))
        st_client_secret = get_input("STRAVA_CLIENT_SECRET", existing_secrets.get("STRAVA_CLIENT_SECRET"))
        
        print("\nTo get a refresh token with proper permissions:")
        print(f"1. Visit: https://www.strava.com/oauth/authorize?client_id={st_client_id}&response_type=code&redirect_uri=http://localhost:5000/exchange_token&approval_prompt=force&scope=read,activity:read,activity:read_all")
        print("2. Authorize and copy the 'code' from the resulting URL (e.g., ?code=abcdef123...)")
        
        auth_code = get_input("Enter the 'code' from the URL (leave blank to use existing refresh token)")
        
        if auth_code:
            print("Exchanging code for a new refresh token...")
            import requests
            resp = requests.post("https://www.strava.com/oauth/token", data={
                "client_id": st_client_id,
                "client_secret": st_client_secret,
                "code": auth_code,
                "grant_type": "authorization_code"
            })
            if resp.status_code == 200:
                data = resp.json()
                st_refresh_token = data.get("refresh_token")
                print(f"Success! New refresh token obtained.")
            else:
                print(f"Failed to exchange code: {resp.status_code} - {resp.text}")
                st_refresh_token = get_input("STRAVA_REFRESH_TOKEN", existing_secrets.get("STRAVA_REFRESH_TOKEN"))
        else:
            st_refresh_token = get_input("STRAVA_REFRESH_TOKEN", existing_secrets.get("STRAVA_REFRESH_TOKEN"))
        
        st_env["STRAVA_CLIENT_ID"] = st_client_id
        st_env["STRAVA_CLIENT_SECRET"] = st_client_secret
        st_env["STRAVA_REFRESH_TOKEN"] = st_refresh_token
        
        # Save to .env for persistence
        with open(env_file, "w") as f:
            f.write(f"STRAVA_CLIENT_ID={st_client_id}\n")
            f.write(f"STRAVA_CLIENT_SECRET={st_client_secret}\n")
            f.write(f"STRAVA_REFRESH_TOKEN={st_refresh_token}\n")

    # 3. Apple Health Data
    print_header("Apple Health Data Import")
    print("Please ensure you have exported 'export.zip' from your iPhone.")
    health_zip = get_input("Path to export.zip (e.g. ~/Downloads/export.zip)", setup_config.get("health_zip"))
    
    if not health_zip:
        print("Skipping Health import (no path provided).")
        do_health = False
    else:
        health_zip = Path(health_zip).expanduser().absolute()
        if not health_zip.exists():
            print(f"Warning: File not found at {health_zip}. Skipping Health import.")
            do_health = False
        else:
            do_health = True
            # Save successful path
            setup_config["health_zip"] = str(health_zip)
            with open(config_file, "w") as f:
                json.dump(setup_config, f)

    # 4. Processing
    print_header("Processing Data...")
    
    # Check for existing data
    has_health = False
    import duckdb
    with duckdb.connect(str(db_path)) as conn:
        try:
            res = conn.execute("SELECT COUNT(*) FROM health_metrics").fetchone()
            has_health = res[0] > 0
        except:
            pass

    # Extract & Parse Health
    if do_health and not has_health:
        print("--- Parsing Apple Health (this may take a minute) ---")
        health_out = reflection_dir / "data" / "health_records.csv"
        # Extract first
        run_cmd(["python3", "health_export.py", "extract", "--file", str(health_zip)], cwd=health_dir)
        
        # Robustly find export.xml
        xml_path = None
        # Try a few common locations relative to the zip
        search_roots = [health_zip.parent, health_dir]
        for root in search_roots:
            for path in root.rglob("export.xml"):
                if "apple_health_export" in str(path) or "apple-health-export" in str(path):
                    xml_path = path
                    break
            if xml_path: break
            
        if xml_path:
            print(f"Found XML at {xml_path}")
            # Target records we care about
            run_cmd(["python3", "health_parser.py", str(xml_path), "export-records", "--output", str(health_out)], cwd=health_dir)
            # Import into reflection
            run_cmd(["python3", "reflect.py", "--database", str(db_path), "import-health", str(health_out)], 
                    cwd=reflection_dir)
        else:
            print("Could not locate export.xml after extraction.")
    elif has_health:
        print("Health data already exists in database. Skipping import.")

    # Pull & Import Strava
    if use_strava:
        print_header("Strava Data Sync")
        print("Note: If you get a 401 error, you may need to re-authorize with correct scopes.")
        print(f"Go to: https://www.strava.com/oauth/authorize?client_id={st_client_id}&response_type=code&redirect_uri=http://localhost:5000/exchange_token&approval_prompt=force&scope=read,activity:read,activity:read_all")
        print("After authorizing, use the 'refresh_token' from the response.")
        
        print("\n--- Pulling Strava Data ---")
        st_export_dir = strava_dir / "strava-export"
        run_cmd(["python3", "strava_pull.py", "--out-dir", str(st_export_dir)], 
                cwd=strava_dir, env=st_env)
        run_cmd(["python3", "reflect.py", "--database", str(db_path), "import-strava", str(st_export_dir)], 
                cwd=reflection_dir)

    # 5. Launch
    print_header("Setup Complete!")
    print("Running initial analysis...")
    run_cmd(["python3", "reflect.py", "--database", str(db_path), "analyze"], 
            cwd=reflection_dir)
    
    print("\nStarting the Reflection UI now on port 5001...")
    try:
        run_cmd(["python3", "reflect.py", "--database", str(db_path), "serve", "--port", "5001", "--debug"], 
                cwd=reflection_dir)
    except KeyboardInterrupt:
        print("\nShutting down. Your data is saved in DuckDB.")

if __name__ == "__main__":
    main()
