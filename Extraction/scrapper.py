import requests
from bs4 import BeautifulSoup

def get_flipkart_data(search_query):
    # Search url seaarch must be   feed  keywords
    url = f'https://www.flipkart.com/search?q={search_query}&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=on&as=off'

    # http request
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:

        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract information about each product
        products = soup.find_all('div', class_='_2kHMtA')

        for product in products:
            
            title = product.find('div', class_='_4rR01T').text.strip()
            price = product.find('div', class_='_30jeq3 _1_WHN1').text.strip()
            # rating = product.find('div', class_='_3LWZlK').text.strip()

            print(f'Title: {title}\nPrice: {price}\n{"-" * 30}')

    else:
        print(f'Error {response.status_code}: Unable to fetch data from Flipkart')

search_query = 'Mobile'
get_flipkart_data(search_query)
