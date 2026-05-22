def check_visa_sponsorship(title, description, country):
    """
    Scans the job title and description text for sponsorship keywords across global target markets.
    Returns a tuple: (Sponsorship_YN, Visa_Type_Details)
    """
    if not isinstance(description, str):
        description = ""
    if not isinstance(title, str):
        title = ""
        
    text_to_scan = (title + " " + description).lower()
    
    # Global explicit sponsorship phrase markers
    sponsorship_keywords = [
        "visa sponsorship", "sponsorship available", "sponsorship provided", 
        "sponsorship offered", "eligible for sponsorship", "work permit sponsorship",
        "open to international applicants", "relocation support", "relocation assistance",
        "international candidates"
    ]
    
    has_sponsor_mention = any(kw in text_to_scan for kw in sponsorship_keywords)
    
    # --- Country-Specific Immigration Systems ---
    if country == "United Kingdom":
        if "skilled worker" in text_to_scan or "tier 2" in text_to_scan:
            return "Y", "Skilled Worker Visa"
        elif has_sponsor_mention:
            return "Y", "UK Work Visa (Unspecified)"
            
    elif country == "Ireland":
        if "critical skills" in text_to_scan or "gsep" in text_to_scan:
            return "Y", "Critical Skills Employment Permit"
        elif "general employment" in text_to_scan:
            return "Y", "General Employment Permit"
        elif has_sponsor_mention:
            return "Y", "Irish Work Permit (Unspecified)"
            
    elif country == "Singapore":
        if "employment pass" in text_to_scan or "e-pass" in text_to_scan or "ep pass" in text_to_scan:
            return "Y", "Employment Pass (EP)"
        elif "s pass" in text_to_scan:
            return "Y", "S Pass"
        elif has_sponsor_mention:
            return "Y", "Singapore Work Pass (Unspecified)"
            
    elif country == "New Zealand":
        # New Zealand uses Accredited Employers to handle worker visa routes
        if "aewv" in text_to_scan or "accredited employer" in text_to_scan or "skilled migrant" in text_to_scan:
            return "Y", "Accredited Employer Work Visa (AEWV)"
        elif has_sponsor_mention:
            return "Y", "NZ Work Visa (Sponsorship Hinted)"
            
    elif country == "Iceland":
        if "expert knowledge" in text_to_scan or "specialised" in text_to_scan or "direcotorate of labour" in text_to_scan:
            return "Y", "Expert Knowledge Residence Permit"
        elif has_sponsor_mention:
            return "Y", "Icelandic Work Permit"
            
    elif country == "Greece":
        if "blue card" in text_to_scan or "digital nomad" in text_to_scan:
            return "Y", "EU Blue Card / Greek Digital Nomad"
        elif has_sponsor_mention:
            return "Y", "Greece Employment Visa"
            
    elif country in ["Netherlands", "Germany", "Spain", "European Union"]:
        if "blue card" in text_to_scan or "highly skilled migrant" in text_to_scan or "kennismigrant" in text_to_scan:
            return "Y", "EU Blue Card / Highly Skilled Migrant"
        elif has_sponsor_mention:
            return "Y", "EU Work Visa (Unspecified)"
            
    elif country == "Thailand":
        if "non-b" in text_to_scan or "work permit" in text_to_scan:
            return "Y", "Non-Immigrant B Visa & Work Permit"
        elif has_sponsor_mention:
            return "Y", "Thailand Work Permit"

    # Catch-all general flag
    if has_sponsor_mention:
        return "Y", "Sponsorship Mentioned"
        
    return "N", "Not Mentioned"

def run_global_tracker():
    search_roles = ["Data Analyst", "Analytics Engineer", "Business Intelligence Analyst"]
    
    # Expanded Country Map
    countries = {
        "uk": "United Kingdom",
        "ireland": "Ireland",
        "singapore": "Singapore",
        "nz": "New Zealand",
        "is": "Iceland",
        "gr": "Greece",
        "nl": "Netherlands",
        "th": "Thailand"
    }
    
    all_scraped_data = []
    print(f"Executing Global Cloud Job Run: {datetime.now().strftime('%Y-%m-%d')}")
    
    for country_code, country_name in countries.items():
        for role in search_roles:
            print(f"Querying listings for '{role}' in {country_name}...")
            try:
                jobs = scrape_jobs(
                    site_name=["linkedin", "indeed"],
                    search_term=role,
                    location=country_name,
                    results_wanted=10,
                    hours_old=48,
                    country_provided=country_code,
                    enforce_desktop=True
                )
                
                if jobs is not None and not jobs.empty:
                    keep_cols = ['site', 'title', 'company', 'job_url', 'location', 'date_posted', 'description']
                    valid_cols = [c for c in keep_cols if c in jobs.columns]
                    jobs = jobs[valid_cols]
                    
                    jobs['search_query'] = role
                    jobs['target_region'] = country_name
                    
                    # Compute visa mappings row by row
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
            except Exception as e:
                print(f"Skipping {role} in {country_name} due to endpoint configuration: {e}")
                continue

    if all_scraped_data:
        master_df = pd.concat(all_scraped_data, ignore_index=True)
        filename = "realtime_job_pipeline.csv"
        
        if os.path.exists(filename):
            historical_df = pd.read_csv(filename)
            combined_df = pd.concat([historical_df, master_df]).drop_duplicates(subset=['job_url'], keep='first')
            combined_df.to_csv(filename, index=False)
            print(f"Sync complete. Unified pipeline length: {len(combined_df)} rows.")
        else:
            master_df.to_csv(filename, index=False)
            print(f"Pipeline created with {len(master_df)} rows.")
    else:
        print("No matches detected in this window timeframe.")
