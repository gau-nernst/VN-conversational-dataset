import os
import requests
from bs4 import BeautifulSoup
import unicodedata

def get_soup_from_url(url):
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, "lxml")
    return soup

def extract_books_from_soup(soup):
    books = soup.find_all("div", class_="ms_list_item")
    books = [x.find("a", class_=None) for x in books]
    urls = [x["href"] for x in books]
    titles = [x.get_text().strip() for x in books]

    return urls, titles

def get_books(author, base_url="https://isach.info/story.php?list=story&author="):
    full_url = base_url + author
    soup = get_soup_from_url(full_url)

    num_pages = soup.find("ul", class_="pagination").find_all("li")[-2].get_text()
    num_pages = int(num_pages)

    # first page
    urls, titles = extract_books_from_soup(soup)

    # rest of the pages
    for i in range(2, num_pages+1):
        url = f"{full_url}&page={i}"
        soup = get_soup_from_url(url)
        new_urls, new_titles = extract_books_from_soup(soup)

        urls.extend(new_urls)
        titles.extend(new_titles)
    
    return urls, titles

def get_texts(url: str):
    soup = get_soup_from_url(url + "&chapter=0000")
    num_chapters = soup.find("a", {"title": "Cách tính số chương"}).get_text()
    num_chapters = int(num_chapters)

    if num_chapters == 1:
        full_text = extract_text(soup)
        yield full_text

    else:
        for i in range(1, num_chapters+1):
            chapter_url = f"{url}&chapter={i:04d}"
            soup = get_soup_from_url(chapter_url)

            chapter = [f"Chapter {i}"]
            chapter.extend(extract_text(soup))
            yield chapter


def extract_text(soup):
    paras = soup.find_all("div", class_="ms_text")
    paras = ["".join(x.find_all(text=True)) for x in paras]

    return paras

def main():
    author = "nguyen nhat anh"
    os.makedirs(author, exist_ok=True)

    # urls, titles = get_books(author)
    
    # for url, title in zip(urls, titles):
    #     chapters = get_texts(url)
    #     filename = unicodedata.normalize(title, "NFD")
    #     filename = title.lower().replace(" ", "-")

    #     path = os.path.join(author, f"{filename}.txt")
    #     with open(path, "w", encoding="utf-8") as f:
    #         for paras in chapters:
    #             for x in paras:
    #                 f.write(x)
    #                 f.write("\n")

    test = "đi qua hoa cúc?---"
    output = "".join([x for x in test if x.isalnum() or x.isspace()])
    # print(output)

    # output = unicodedata.normalize("NFKD", test).encode("ascii", "ignore")
    print(output)

if __name__ == "__main__":
    main()