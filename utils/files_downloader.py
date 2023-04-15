import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import zipfile

def download_data(url_suffix):
    """
    Web scrapes the website https://transitfeeds.com/ in order to find applicable GTFS download data.
    
    The retrieved data is unzipped, processed, and saved to their respective local directories.

    Data is stored in ./files/ under /zip_files/ which is then extracted into /extracted/
    """

    # Prepare data retrieval via web sraping
    base_url = 'https://transitfeeds.com/'
    url = base_url + url_suffix
    response = requests.get(url)
    total_pages = 0

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        pagination = soup.find("ul", class_="pagination")
        last_page_link = pagination.find_all("a")[-1] # second to last link
        total_pages = int(last_page_link.text)
        print(f"Total number of pages: {total_pages}")
    else:
        print(f"Failed to retrieve data from {url}. Status code: {response.status_code}")

    # Iterate through all pages from until total_pages
    a_text_list = []

    for page_num in range(1, total_pages + 1):
        url = f'https://transitfeeds.com/p/mta/79?p={page_num}'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the first tbody element
        tbody_element = soup.find('tbody')

        # Find all tr elements within the current tbody
        tr_elements = tbody_element.find_all('tr')

        # Loop through each tr element
        for tr in tr_elements:
            # Find the first td element within the current tr
            td_element = tr.find('td')

            # Find the first a element within the current td and extract its text
            a_element = td_element.find('a')
            a_text = a_element.text

            # Convert the date string to the desired format
            date_obj = datetime.strptime(a_text, '%d %B %Y')
            formatted_date = date_obj.strftime('%Y%m%d')
            a_text_list.append(formatted_date)

            # Find the second td element within the current tr
            td_element_2 = tr.find_all('td')[3]

            # Find the second a element within the current td and extract its href
            a_element_2 = td_element_2.find_all('a')[1]
            a_href = a_element_2.get('href')

            # Check if the files/zip_files and files/extracted directories exist
            if not os.path.exists('files/zip_files'):
                os.makedirs('files/zip_files')
            if not os.path.exists('files/extracted'):
                os.makedirs('files/extracted')

            # Download the zip file and save it to files/zip_files
            zip_file_path = f'files/zip_files/{formatted_date}.zip'
            if os.path.exists(zip_file_path):
                print(f'{formatted_date}.zip already exists.')
            else:
                # print(f'Downloading {formatted_date}.zip...')
                url = f'{base_url}{a_href}'
                response = requests.get(url)
                if response.status_code == 200:
                    with open(zip_file_path, 'wb') as f:
                        f.write(response.content)
                    print(f'{formatted_date}.zip downloaded successfully.')
                else:
                    print(f'Error downloading {formatted_date}.zip. Status code: {response.status_code}')

            # Extract the zip file to files/extracted
            extracted_folder_path = f'files/extracted/{formatted_date}'
            if os.path.exists(extracted_folder_path):
                print(f'{formatted_date} folder already exists.')
            else:
                # print(f'Extracting {formatted_date}.zip...')
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    zip_ref.extractall(extracted_folder_path)
                print(f'{formatted_date}.zip extracted successfully.')
