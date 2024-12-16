from bs4 import BeautifulSoup

def load_full_seach_page() -> BeautifulSoup:

    with open("example_google_search_w_kwargs.html") as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    
    return soup