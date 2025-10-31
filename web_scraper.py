import requests
from bs4 import BeautifulSoup
import csv
from urllib.parse import urljoin
import re
from datetime import datetime
import sys
from collections import defaultdict
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from difflib import SequenceMatcher

# caden finley

csv_fields = ['Hymnal_Code', 'Hymnal_Name', 'Denomination', 'Hymn_Total', 'Hymn_Number', 'Hymn', 'Hymn_No_Blanks', 'Hymn_ID']
base_url = 'https://hymnary.org/'
sub_url_hymnal = 'hymnal'
timestamp = datetime.now().strftime("%m-%d-%Y")
csv_filename = f'hymnal_data_{timestamp}.csv'
global_hymn_id = 1
request_counter = 0
hymnals_to_search = []
request_delay = 0.5
max_workers = 5
hymn_id_lock = threading.Lock()
request_counter_lock = threading.Lock()
similarity_threshold = 0.85
max_similarity_results = 15

def get_response(url):
    global request_counter
    with request_counter_lock:
        request_counter += 1
        current_count = request_counter
    print(f"Making request #{current_count}")
    time.sleep(request_delay)
    return requests.get(url, timeout=10)

def extract_pager_items(soup, base_hymnal_url):
    pager_items = []
    page_0_href = f"{base_hymnal_url}?page=0"
    pager_items.append(('0', page_0_href))

    max_page = 0
    for pager_list in soup.find_all('ul', class_='pager'):
        pager_last = pager_list.find('li', class_='pager-last')
        if pager_last:
            link = pager_last.find('a')
            if link and link.has_attr('href'):
                href = link['href']
                page_match = re.search(r'page=(\d+)', href)
                if page_match:
                    max_page = int(page_match.group(1))
                    break
                
    for page_num in range(1, max_page + 1):
        page_href = f"{base_hymnal_url}?page={page_num}"
        pager_items.append((str(page_num), page_href))

    unique_items = []
    seen = set()
    for label, href in pager_items:
        key = (label, href)
        if key in seen:
            continue
        seen.add(key)
        unique_items.append({'label': label, 'href': href})

    return unique_items

def extract_hymnal_metadata(soup):
    hymnal_name = ""
    denomination = ""
    
    tabs_wrapper = soup.find('div', id='tabs-wrapper')
    if tabs_wrapper:
        page_title_div = tabs_wrapper.find('div', class_='page-title')
        if page_title_div:
            h1 = page_title_div.find('h1')
            if h1:
                hymnal_name = h1.get_text(strip=True)

    info_table = soup.find('table', class_='infoTable')
    if info_table:
        for row in info_table.find_all('tr', class_='result-row'):
            label_cell = row.find('span', class_='hy_infoLabel')
            if label_cell and 'Denomination:' in label_cell.get_text():
                value_cell = row.find('span', class_='hy_infoItem')
                if value_cell:
                    link = value_cell.find('a')
                    if link:
                        denomination = link.get_text(strip=True)
                    else:
                        denomination = value_cell.get_text(strip=True)
                    break
    
    return hymnal_name, denomination

def format_hymn_no_blanks(hymn_text): 
    formatted = re.sub(r'\s+', '_', hymn_text.strip())
    formatted = re.sub(r'[^a-zA-Z0-9_]', '', formatted)
    return formatted

def extract_hymns_from_page(soup, page_label, hymnal_code, hymnal_name, denomination):
    global global_hymn_id
    
    list_anchor = soup.find('a', {'name': 'list'})
    if list_anchor:
        table = list_anchor.find_next('table')
    else:
        tables = soup.find_all('table')
        table = None
        for t in tables:
            header_row = t.find('tr')
            if header_row:
                headers = header_row.find_all(['th', 'td'])
                if len(headers) >= 7:
                    table = t
                    break
    
    if not table:
        print(f"  No table found on page {page_label}")
        return []
    
    rows = table.find_all('tr')
    if len(rows) <= 1:
        print(f"  No data rows found on page {page_label}")
        return []
    
    page_hymns = []
    for i, row in enumerate(rows[1:], 1):
        cells = row.find_all('td')

        if len(cells) < 3:
            continue
            
        hymn_number_cell = cells[0]
        hymn_number_link = hymn_number_cell.find('a')
        hymn_number_raw = hymn_number_link.get_text(strip=True) if hymn_number_link else ''
        hymn_number = re.sub(r'[^0-9]', '', hymn_number_raw)
        
        hymn_text_cell = cells[1]
        hymn_text_link = hymn_text_cell.find('a')
        hymn_text = hymn_text_link.get_text(strip=True) if hymn_text_link else ''

        if hymn_number and hymn_text:
            with hymn_id_lock:
                hymn_id = global_hymn_id
                global_hymn_id += 1
            
            page_hymns.append({
                'Hymnal_Code': hymnal_code,
                'Hymnal_Name': hymnal_name,
                'Denomination': denomination,
                'Hymn_Total': '',
                'Hymn_Number': hymn_number,
                'Hymn': hymn_text,
                'Hymn_No_Blanks': format_hymn_no_blanks(hymn_text),
                'Hymn_ID': hymn_id
            })
    
    print(f"  Found {len(page_hymns)} hymns on page {page_label}")
    return page_hymns

def extract_all_hymn_data(initial_soup, pager_items, hymnal_code, hymnal_name, denomination):
    all_hymns = []
    page_0_hymns = extract_hymns_from_page(initial_soup, "0", hymnal_code, hymnal_name, denomination)
    all_hymns.extend(page_0_hymns)

    for item in pager_items:
        if item['label'] == '0':
            continue
            
        page_url = item['href']
        if not page_url:
            continue
        
        page_response = get_response(page_url)
        if page_response.status_code != 200:
            print(f"  Failed to fetch page {page_url}")
            continue
            
        page_soup = BeautifulSoup(page_response.text, 'html.parser')
        page_hymns = extract_hymns_from_page(page_soup, item['label'], hymnal_code, hymnal_name, denomination)
        all_hymns.extend(page_hymns)
    
    hymn_total = len(all_hymns)
    for hymn in all_hymns:
        hymn['Hymn_Total'] = hymn_total

    print(f"Total hymns collected for {hymnal_code}: {hymn_total}", end='\n')
    
    return all_hymns

def generate_hymnals_csv(all_hymns_data, output_csv):
    hymnals = {}
    
    for row in all_hymns_data:
        code = row['Hymnal_Code']
        if code not in hymnals:
            hymnals[code] = {
                'Hymnal_Code': code,
                'Hymnal_Name': row['Hymnal_Name'],
                'Denomination': row['Denomination'],
                'Hymn_Total': row['Hymn_Total']
            }
    
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['Hymnal_Code', 'Hymnal_Name', 'Denomination', 'Hymn_Total']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        
        for code in sorted(hymnals.keys()):
            writer.writerow(hymnals[code])
    print(f"Data written to {output_csv}")

def generate_book_data_csv(all_hymns_data, output_csv):
    hymnal_codes = set()
    hymn_data = defaultdict(lambda: defaultdict(int))
    
    for row in all_hymns_data:
        code = row['Hymnal_Code']
        hymn = row['Hymn']
        
        hymnal_codes.add(code)
        hymn_data[hymn][code] += 1
      
    sorted_codes = sorted(hymnal_codes)
     
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['Hymn', 'Hymn_No_Blanks'] + sorted_codes + ['Total']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        
        for hymn in sorted(hymn_data.keys()):
            row_data = {'Hymn': hymn, 'Hymn_No_Blanks': format_hymn_no_blanks(hymn)}
            row_total = 0
            
            for code in sorted_codes:
                count = hymn_data[hymn][code]
                row_data[code] = count
                row_total += count
            
            row_data['Total'] = row_total
            writer.writerow(row_data)
        
        
        total_row = {'Hymn': 'TOTAL', 'Hymn_No_Blanks': 'TOTAL'}
        grand_total = 0
        
        for code in sorted_codes:
            code_total = sum(hymn_data[hymn][code] for hymn in hymn_data)
            total_row[code] = code_total  # type: ignore
            grand_total += code_total
        
        total_row['Total'] = grand_total  # type: ignore
        writer.writerow(total_row)
        print(f"Data written to {output_csv}")


def generate_hymn_similarity_csv(all_hymns_data, output_csv, threshold=0.85, max_results_per_hymn=15):
    """Generate a CSV showing fuzzy matches between hymns across hymnals."""
    if not all_hymns_data:
        return

    normalized_entries = []
    for row in all_hymns_data:
        hymn_text = row.get('Hymn', '').strip()
        if not hymn_text:
            continue

        normalized_text = re.sub(r'\s+', ' ', hymn_text).lower()
        normalized_entries.append({
            'Hymn': hymn_text,
            'Hymn_No_Blanks': row.get('Hymn_No_Blanks', ''),
            'Hymnal_Code': row.get('Hymnal_Code', ''),
            'Hymnal_Name': row.get('Hymnal_Name', ''),
            'Denomination': row.get('Denomination', ''),
            'Hymn_ID': row.get('Hymn_ID'),
            'Normalized_Hymn': normalized_text
        })

    if not normalized_entries:
        return

    unique_map = {}
    for entry in normalized_entries:
        key = entry['Normalized_Hymn']
        bucket = unique_map.setdefault(key, {
            'text': entry['Hymn'],
            'compare_text': entry['Hymn'].lower(),
            'entries': []
        })
        bucket['entries'].append(entry)

    unique_keys = list(unique_map.keys())
    similarity_results = {key: [(key, 1.0)] for key in unique_keys}

    for i in range(len(unique_keys)):
        key_i = unique_keys[i]
        text_i = unique_map[key_i]['compare_text']
        for j in range(i + 1, len(unique_keys)):
            key_j = unique_keys[j]
            text_j = unique_map[key_j]['compare_text']
            score = SequenceMatcher(None, text_i, text_j).ratio()
            if score >= threshold:
                similarity_results[key_i].append((key_j, score))
                similarity_results[key_j].append((key_i, score))

    similarity_rows = []
    for entry in normalized_entries:
        key = entry['Normalized_Hymn']
        matches = similarity_results.get(key, [])
        matches = sorted(matches, key=lambda item: item[1], reverse=True)

        similar_entries = []
        seen_pairs = set()
        base_identifier = (entry['Hymn'], entry['Hymnal_Code'])
        seen_pairs.add(base_identifier)

        for similar_key, score in matches:
            for similar_entry in unique_map[similar_key]['entries']:
                identifier = (similar_entry['Hymn'], similar_entry['Hymnal_Code'])
                if identifier in seen_pairs:
                    continue

                similar_entries.append(
                    f"{similar_entry['Hymn']} [{similar_entry['Hymnal_Code']}] ({score:.2f})"
                )
                seen_pairs.add(identifier)

                if max_results_per_hymn and len(similar_entries) >= max_results_per_hymn:
                    break

            if max_results_per_hymn and len(similar_entries) >= max_results_per_hymn:
                break

        if similar_entries:
            similarity_rows.append({
                'Base_Hymn': entry['Hymn'],
                'Base_Hymn_No_Blanks': entry['Hymn_No_Blanks'],
                'Base_Hymnal_Code': entry['Hymnal_Code'],
                'Base_Hymnal_Name': entry['Hymnal_Name'],
                'Base_Denomination': entry['Denomination'],
                'Similar_Hymns': '; '.join(similar_entries),
                'Similar_Hymn_Count': len(similar_entries)
            })

    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'Base_Hymn',
            'Base_Hymn_No_Blanks',
            'Base_Hymnal_Code',
            'Base_Hymnal_Name',
            'Base_Denomination',
            'Similar_Hymns',
            'Similar_Hymn_Count'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(similarity_rows)

    print(f"Data written to {output_csv}")

def process_single_hymnal(hymnal_code):
    """Process a single hymnal and return its hymn data."""
    target_url = urljoin(base_url, f"{sub_url_hymnal}/{hymnal_code}")
    response = get_response(target_url)

    if response.status_code != 200:
        print(f"Failed to fetch {target_url} (status {response.status_code})")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')

    hymnal_name, denomination = extract_hymnal_metadata(soup)
    print(f"Processing {hymnal_code}: {hymnal_name}")
    
    pager_items = extract_pager_items(soup, target_url)
    
    hymns = extract_all_hymn_data(soup, pager_items, hymnal_code, hymnal_name, denomination)
    
    return hymns

def main():
    global hymnals_to_search, request_delay, max_workers
    
    if len(sys.argv) < 4 or not sys.argv[1].isdigit() or not re.match(r'^\d+(\.\d+)?$', sys.argv[2]):
        print("Usage: python3 web_scraper.py <workers> <delay> <hymnal1> <hymnal2> ...")
        print("Example: python3 web_scraper.py 5 0.5 SoP1870 GSC1986 SFP1994")
        sys.exit(1)
    
    max_workers = int(sys.argv[1])
    request_delay = float(sys.argv[2])
    hymnals_to_search = sys.argv[3:]

    print(f"Searching hymnals: {', '.join(hymnals_to_search)}")
    print(f"Request delay: {request_delay}s, Workers: {max_workers}")
    print()
    
    all_hymns = []
    max_workers = min(5, len(hymnals_to_search))
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_hymnal = {executor.submit(process_single_hymnal, code): code 
                           for code in hymnals_to_search}
        for future in as_completed(future_to_hymnal):
            hymnal_code = future_to_hymnal[future]
            try:
                hymns = future.result()
                all_hymns.extend(hymns)
            except Exception as exc:
                print(f"{hymnal_code} generated an exception: {exc}")
    
    print(f"Total hymns collected: {len(all_hymns)}")
    print(f"Total HTTP requests made: {request_counter}")
    
    if all_hymns:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_fields)
            writer.writeheader()
            writer.writerows(all_hymns)
        print(f"Data written to {csv_filename}")
        
        hymnals_csv = f'hymnals_{timestamp}.csv'
        book_data_csv = f'book_data_{timestamp}.csv'
        similarity_csv = f'hymn_similarity_{timestamp}.csv'
        generate_hymnals_csv(all_hymns, hymnals_csv)
        generate_book_data_csv(all_hymns, book_data_csv)
        generate_hymn_similarity_csv(
            all_hymns,
            similarity_csv,
            threshold=similarity_threshold,
            max_results_per_hymn=max_similarity_results
        )

if __name__ == "__main__":
    main()
