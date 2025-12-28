from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import requests
def check_url_exists(url):
    try:
        response = requests.head(url, timeout=5)
        return url if response.status_code == 200 else None
    except:
        return None

def extract_date_from_url(url):
    match = re.search(r'/(\d{8})_Viajes_distritos', url)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%d').date()
    return None

def fetch_mitma_url(start_date:datetime,end_date:datetime):
    try:
        #I need to be sure that they are dates of type datetime  
        if isinstance(start_date, str):
            # Convert string 'YYYY-MM-DD' to datetime
            start = datetime.strptime(start_date, "%Y-%m-%d")
        elif isinstance(start_date, datetime):
            # It's already a datetime object, use it directly
            start = start_date
        else:
            raise ValueError("start_date must be a string ('YYYY-MM-DD') or datetime object.")

        # 2. Normalize end_date
        if isinstance(end_date, str):
            end = datetime.strptime(end_date, "%Y-%m-%d")
        elif isinstance(end_date, datetime):
            end = end_date
        else:
            raise ValueError("end_date must be a string ('YYYY-MM-DD') or datetime object.")
        
        
        if start > end:
            raise ValueError(f"Start date ({start.date()}) cannot be after end date ({end.date()}).")
        
        dates = [start + timedelta(days=x) for x in range((end - start).days + 1)]
        
        all_urls = [
            f"https://movilidad-opendata.mitma.es/estudios_basicos/por-distritos/viajes/ficheros-diarios/{d.strftime('%Y-%m')}/{d.strftime('%Y%m%d')}_Viajes_distritos.csv.gz" 
            for d in dates
        ]
        
        print(f"Checking {len(all_urls)} URLs...")
        
        valid_urls = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(check_url_exists, url): url for url in all_urls}
            for i, future in enumerate(as_completed(futures), 1):
                if i % 50 == 0:
                    print(f"Checked {i}/{len(all_urls)} URLs...")
                result = future.result()
                if result:
                    valid_urls.append(result)
        
        print(f"Found {len(valid_urls)} valid URLs")
        
        return valid_urls
    except Exception :
        print("ERROR: Exception fetching the URLs")
        print(f"Start date:{start}  \n End date:{end}")