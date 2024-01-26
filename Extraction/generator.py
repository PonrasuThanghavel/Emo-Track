import requests
from bs4 import BeautifulSoup
import json
import csv
from itertools import count

def get_flipkart_data(search_query):
    # load the class names according to the search query
    with open('classnames.json') as json_file: 
        class_data = json.load(json_file)

    search_query = search_query.lower() # lower case

    class_names = next((item for item in class_data if item["keywords"].lower() == search_query), None)

    if class_names is None:
        print("No class names found for the given search query.")
        return

    base_url = 'https://www.flipkart.com/search?q='
    page_number = 1
    id_counter = count(1) # id counter
    filename = search_query.replace(' ', '_') + '_list.csv' #remove the whitespace any in search query a save file name

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['ID', 'Product Name', 'Product Link']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        while True:
            url = f'{base_url}{search_query}&page={page_number}'
            response = requests.get(url)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                products = soup.find_all('div', class_=class_names["product_finder"]) 

                if not products:
                    break

                for product in products:
                    title_div = product.find('div', class_=class_names["title_class"]) # if title in div 
                    if title_div:
                        title = title_div.text.strip()
                    else:
                        title_anchor = product.find('a', class_=class_names["title_class"])
                        if title_anchor:
                            title = title_anchor.text.strip()
                        else:
                            title = "Title Not Found"

                    product_link = product.find('a', class_=class_names["product_link"])['href'].strip()

                    writer.writerow({'ID': next(id_counter), 'Product Name': title, 'Product Link': product_link})

                page_number += 1
            else:
                print(f'Error {response.status_code}: Unable to fetch data from Flipkart')
                break

search_query = 'mobile'  
get_flipkart_data(search_query)
