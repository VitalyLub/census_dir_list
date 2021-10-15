import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime




def get_all_links_in_page(base_link, history):
    father_folder = base_link.split('/')[-3]
    page = requests.get(base_link)
    soup = BeautifulSoup(page.content, "html.parser")
    the_table = soup.find_all("table")
    if len(the_table) > 0:
        the_table = the_table[0]
        links_in_page = the_table.find_all('a')
        final_files = []
        more_links = []
        
        for link in links_in_page:
            if link['href'] not in ('?C=D;O=A', '?C=N;O=D', '?C=M;O=A', '?C=S;O=A', '/'+father_folder+'/'):
                if '.' in link['href']:
                    final_files.append(urljoin(base_link, link['href']))
                else:
                    if urljoin(base_link, link['href']) not in history:
                        more_links.append(urljoin(base_link, link['href']))
        return final_files, more_links
    return [], []
    



def multi_get_data(workers, url_folder_to_scrape):
    start_time = datetime.now()
    # Execute our get_data in multiple threads each having a different page number
    with ThreadPoolExecutor(max_workers=workers) as executor:
        all_links_to_scrape = [url_folder_to_scrape]
        all_final_files = []
        history = []
        while len(all_links_to_scrape) > 0:    
            link_to_work_on = all_links_to_scrape[0]
            all_links_to_scrape = [x for x in all_links_to_scrape if x != link_to_work_on]
            history.append(link_to_work_on)
            print("WORK ON:", link_to_work_on)
            final_files, more_links = get_all_links_in_page(link_to_work_on, history)
            all_final_files = all_final_files + final_files
            all_links_to_scrape = all_links_to_scrape + more_links
            print("final_files:", len(all_final_files))
            print("links_to_check:", len(all_links_to_scrape))
    print(f'Time take {datetime.now() -     start_time}')
    return all_final_files



if __name__ == '__main__':
    file_dest_path = sys.argv[1] #example: C://Users//USER//Desktop//blah.csv
    url_folder_to_scrape = sys.argv[2] # example: https://www2.census.gov/retail/forms_and_letters/
    
    all_final_files = multi_get_data(20, url_folder_to_scrape)

    final_final = [[val] for val in all_final_files]
    with open(file_dest_path, 'w', newline='', encoding="utf-8") as myfile:
         wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
         wr.writerows(final_final)