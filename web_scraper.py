import requests
from bs4 import BeautifulSoup
import csv
from urllib.parse import urljoin
import re
from datetime import datetime
import sys
import time

#caden finley

csv_fields = ['Hymnal_Code', 'Hymnal_Name', 'Denomination', 'Hymn_Total', 'Hymn_Number', 'Hymn', 'Hymn_No_Blanks', 'Hymn_ID']
base_url = 'https://hymnary.org/'
sub_url_hymnal = 'hymnal'
timestamp = datetime.now().strftime("%m-%d-%Y")
csv_filename = f'hymnal_data_{timestamp}.csv'
global_hymn_id = 1
request_counter = 0
#hymnals_to_search = ['SoP1870', 'GSC1986', 'SFP1994']
hymnals_to_search = []

def get_response(url):
    global request_counter
    request_counter += 1
    print(f"Making request #{request_counter} to: {url}")
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
            page_hymns.append({
                'Hymnal_Code': hymnal_code,
                'Hymnal_Name': hymnal_name,
                'Denomination': denomination,
                'Hymn_Number': hymn_number,
                'Hymn': hymn_text,
                'Hymn_No_Blanks': format_hymn_no_blanks(hymn_text),
                'Hymn_ID': global_hymn_id
            })
            global_hymn_id += 1
    
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
    
    return all_hymns


def main():
    global hymnals_to_search
    if len(sys.argv) < 2 and hymnals_to_search == []:
        sys.exit(1)
    
    hymnals_to_search = sys.argv[1:]
    print(f"Searching hymnals: {', '.join(hymnals_to_search)}")
    print()
    
    all_hymns = []
    
    for hymnal_code in hymnals_to_search:
        target_url = urljoin(base_url, f"{sub_url_hymnal}/{hymnal_code}")
        response = get_response(target_url)

        if response.status_code != 200:
            print(f"Failed to fetch {target_url} (status {response.status_code})")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')

        hymnal_name, denomination = extract_hymnal_metadata(soup)
        print(f"Processing {hymnal_code}: {hymnal_name}")
        
        pager_items = extract_pager_items(soup, target_url)
        
        hymns = extract_all_hymn_data(soup, pager_items, hymnal_code, hymnal_name, denomination)
        
        all_hymns.extend(hymns)
        
        print(f"Total hymns collected for {hymnal_code}: {len([h for h in all_hymns if h['Hymnal_Code'] == hymnal_code])}")
        print()
    
    print(f"Total hymns collected: {len(all_hymns)}")
    print(f"Total HTTP requests made: {request_counter}")
    
    if all_hymns:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_fields)
            writer.writeheader()
            writer.writerows(all_hymns)
        print(f"Data written to {csv_filename}")

if __name__ == "__main__":
    main()
