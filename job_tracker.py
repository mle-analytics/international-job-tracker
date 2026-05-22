import os
import pandas as pd
from datetime import datetime
from jobspy import scrape_jobs

def run_global_tracker():
    # Tailored roles matching your exact profile strengths
    search_roles = ["Data Analyst", "Analytics Engineer", "Business Intelligence Analyst"]
    
    # Target countries requested
    countries = {
        "uk": "United Kingdom",
        "ireland": "Ireland",
        "singapore": "Singapore",
        "thailand": "Thailand",
        "netherlands": "Netherlands",  # Representative EU Hub
        "germany": "Germany"           # Representative EU Hub
    }
    
    all_scraped_data = []
    print(f"Executing Cloud Job Query Run: {datetime.now().strftime('%Y-%m-%d')}")
    
    for country_code, country_name in countries.items():
        for role in search_roles:
            print(f"Querying listings for '{role}' in {country_name}...")
            try:
                # Scrapes Indeed and LinkedIn simultaneously
                jobs = scrape_jobs(
                    site_name=["indeed", "linkedin"],
                    search_term=role,
                    location=country_name,
                    results_wanted=10,  # Optimized limit to avoid anti-bot trigger locks
                    hours_old=48,       # Targets recent postings
                    country_provided=country_code
                )
                
                if jobs is not None and not jobs.empty:
                    # Isolate clean data targets
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
        
        # Append logic: merge fresh data with old history and drop duplicates
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
