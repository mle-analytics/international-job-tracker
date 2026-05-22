import os
import pandas as pd
from datetime import datetime
from jobspy import scrape_jobs

def check_visa_sponsorship(title, description, country):
    if not isinstance(description, str):
        description = ""
    if not isinstance(title, str):
        title = ""
        
    text_to_scan = (title + " " + description).lower()
    
    sponsorship_keywords = [
        "visa sponsorship", "sponsorship available", "sponsorship provided", 
        "sponsorship offered", "eligible for sponsorship", "work permit sponsorship",
        "open to international applicants", "relocation support", "relocation assistance",
        "international candidates", "visa support"
    ]
    
    has_sponsor_mention = any(kw in text_to_scan for kw in sponsorship_keywords)
    
    if country == "United Kingdom":
        if "skilled worker" in text_to_scan or "tier 2" in text_to_scan:
            return "Y", "Skilled Worker Visa"
        elif has_sponsor_mention:
            return "Y", "UK Work Visa (Unspecified)"
    elif country == "Ireland":
        if "critical skills" in text_to_scan or "gsep" in text_to_scan:
            return "Y", "Critical Skills Employment Permit"
        elif has_sponsor_mention:
            return "Y", "Irish Work Permit"
    elif country == "Singapore":
        if "employment pass" in text_to_scan or "e-pass" in text_to_scan or "ep pass" in text_to_scan:
            return "Y", "Employment Pass (EP)"
        elif "s pass" in text_to_scan:
            return "Y", "S Pass"
    elif country == "New Zealand":
        if "aewv" in text_to_scan or "accredited employer" in text_to_scan or "skilled migrant" in text_to_scan:
            return "Y", "Accredited Employer Work Visa (AEWV)"
    elif country == "Iceland":
        if "expert knowledge" in text_to_scan:
            return "Y", "Expert Knowledge Residence Permit"
    elif country == "Greece":
        if "blue card" in text_to_scan or "digital nomad" in text_to_scan:
            return "Y", "EU Blue Card / Greek Digital Nomad"
            
    if has_sponsor_mention:
        return "Y", "Sponsorship Mentioned"
        
    return "N", "Not Mentioned"

def run_global_tracker():
    # Widened the scope slightly to catch more roles matching your profile
    search_roles = ["Data Analyst", "Analytics Engineer", "Business Intelligence"]
    
    countries = {
        "uk": "United Kingdom",
        "ireland": "Ireland",
        "singapore": "Singapore",
        "nz": "New Zealand",
        "iceland": "Iceland",
        "greece": "Greece",
        "netherlands": "Netherlands",
        "thailand": "Thailand"
    }
    
    all_scraped_data = []
    print(f"--- STARTING GLOBAL PIPELINE CYCLE: {datetime.now().strftime('%Y-%m-%d')} ---")
    
    for country_code, country_name in countries.items():
        for role in search_roles:
            print(f"Scraping Engine active for: {role} in {country_name}...")
            try:
                # REMOVED hours_old filter so it grabs historical listings to force file update
                jobs = scrape_jobs(
                    site_name=["linkedin", "indeed"],
                    search_term=role,
                    location=country_name,
                    results_wanted=5,
                    country_indeed=country_code
                )
                
                if jobs is not None and not jobs.empty:
                    print(f"   Success! Found {len(jobs)} rows for {role} in {country_name}")
                    keep_cols = ['site', 'title', 'company', 'job_url', 'location', 'date_posted', 'description']
                    valid_cols = [c for c in keep_cols if c in jobs.columns]
                    jobs = jobs[valid_cols]
                    
                    jobs['search_query'] = role
                    jobs['target_region'] = country_name
                    
                    # Compute custom columns
                    visa_results = jobs.apply(
                        lambda row: check_visa_sponsorship(
                            row.get('title', ''), 
                            row.get('description', ''), 
                            country_name
                        ), axis=1
                    )
                    
                    jobs['visa_sponsorship'] = [r[0] for r in visa_results]
                    jobs['visa_type'] = [r[1] for r in visa_results]
                    
                    if 'description' in jobs.columns:
                        jobs = jobs.drop(columns=['description'])
                        
                    all_scraped_data.append(jobs)
                else:
                    print(f"   0 postings returned for {role} in {country_name}")
            except Exception as e:
                print(f"   Skipped {role} in {country_name} due to terminal block: {e}")
                continue

    if all_scraped_data:
        master_df = pd.concat(all_scraped_data, ignore_index=True)
        filename = "realtime_job_pipeline.csv"
        
        # Completely overwrite the file this run to force the new columns to show up
        master_df.to_csv(filename, index=False)
        print(f"\n--- SUCCESS: File rewritten with {len(master_df)} rows and new columns! ---")
    else:
        print("\n--- CRITICAL: Scraper returned absolutely zero data rows across all targets. ---")

if __name__ == "__main__":
    run_global_tracker()
