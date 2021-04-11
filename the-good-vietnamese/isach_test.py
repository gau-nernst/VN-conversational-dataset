import bs4

from isach import get_soup_from_url

def test_get_soup_from_url():
    url = "https://example.com/"
    soup = get_soup_from_url(url)
    assert type(soup) == bs4.BeautifulSoup

    title = soup.find("h1")
    assert title.get_text() == "Example Domain"