import os
import pandas as pd
from datetime import datetime
from jobspy import scrape_jobs

def run_global_tracker():
    search_roles = ["Data Analyst", "Analytics Engineer", "Business Intelligence Analyst"]
    
    countries = {
        "uk": "United Kingdom",
        "ireland": "Ireland",
        "singapore": "Singapore",
        "thailand": "Thailand",
        "netherlands": "Netherlands",
        "germany": "Germany"
    }
    
    all_scraped_data = []
    print(f"Executing Cloud Job Query Run: {datetime.now().strftime('%Y-%m-%d')}")
    
    for country_code, country_name in countries.items():
        for role in search_roles:
            print(f"Querying listings for '{role}' in {country_name}...")
            try:
                # Scrapes Indeed and LinkedIn simultaneously with desktop masking enabled
                jobs = scrape_jobs(
                    site_name=["linkedin", "indeed"],
                    search_term=role,
                    location=country_name,
                    results_wanted=10,
                    hours_old=48,
                    country_provided=country_code,
                    enforce_desktop=True  # Helps bypass mobile bot-detection configurations
                )
                
                if jobs is not None and not jobs.empty:
                    jobs = jobs[['site', 'title', 'company', 'job_url', 'location', 'date_posted']]
                    jobs['search_query'] = role
                    jobs['target_region'] = country_name
                    all_scraped_data.append(jobs)
            except Exception as e:
                print(f"Skipping {role} in {country_name} due to api/network limits: {e}")
                continue

    if all_scraped_data:
        master_df = pd.concat(all_scraped_data, ignore_index=True)
        filename = "realtime_job_pipeline.csv"
        
        if os.path.exists(filename):
            historical_df = pd.read_csv(filename)
            combined_df = pd.concat([historical_df, master_df]).drop_duplicates(subset=['job_url'], keep='first')
            combined_df.to_csv(filename, index=False)
            print(f"Database updated successfully. Current tracking volume: {len(combined_df)} rows.")
        else:
            master_df.to_csv(filename, index=False)
            print(f"Initial tracking pipeline established with {len(master_df)} fresh rows.")
    else:
        print("No matches detected in this window timeframe.")

if __name__ == "__main__":
    run_global_tracker()
