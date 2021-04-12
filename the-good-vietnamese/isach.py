import os
import re
import asyncio, aiohttp
from bs4 import BeautifulSoup
import unicodedata
import logging
from datetime import datetime

# hack to import from parent directory
import sys
current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
del current_dir
del parent_dir

from utils import Tracker

# pre-compiled regex
non_alphanumeric_regex = re.compile(r"[^\w\d\s]+")
whitespace_regex = re.compile(r"\s+")

# custom csv writer to support appending to existing csv file
class CSVWriter:
    def __init__(self, filename, columns, path="./"):
        self.full_path = os.path.join(path, filename) + ".csv"
        self.num_columns = len(columns)

        # create a new file if not exist
        if not os.path.exists(self.full_path):
            with open(self.full_path, "w", encoding="utf-8") as f:
                f.write(",".join(columns))
                f.write("\n")
        
    def write_rows(self, rows):
        with open(self.full_path, "a", encoding="utf-8") as f:
            for row in rows:
                # string is not sanitized for csv
                # no error checking if each row matches the column
                f.write(",".join([str(x) for x in row]))
                f.write("\n")

# remove accent and convert vietnamese alphabet to english alphabet
# use delimiter to replace whitespace
def sanitize_vn(text, delimiter="-"):
    text = unicodedata.normalize("NFD", text.lower())
    text = "".join([x for x in text if not unicodedata.combining(x)])
    text = text.replace("đ", "d")
    text = non_alphanumeric_regex.sub(" ", text)
    text = whitespace_regex.sub(" ", text)
    text = text.strip().replace(" ", delimiter)

    return text

# client to handle network requests
# wrap around aiohttp session
class AsyncClient():
    def __init__(self):
        connector = aiohttp.TCPConnector(limit_per_host=20)
        timeout = aiohttp.ClientTimeout(total=0)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)

    async def get_soup_from_url(self, url, params=None):
        async with self.session.get(url, params=params) as resp:
            soup = BeautifulSoup(await resp.text(), "lxml")
        return soup

    async def close(self):
        return await self.session.close()

def extract_books_from_soup(soup):
    books = soup.find_all("div", class_="ms_list_item")
    books = [x.find("a", class_=None) for x in books]

    urls = [x["href"] for x in books]
    urls = ["https://isach.info" + x if not x.startswith("http") else x for x in urls]
    titles = [x.get_text().strip() for x in books]

    return urls, titles

async def get_books(author, base_url="https://isach.info/", client=None, book_type="story"):
    need_close = False
    if not client:
        client = AsyncClient()
        need_close = True

    full_url = f"{base_url}{book_type}.php"
    params = {
        "list": book_type,
        "author": author
    }
    soup = await client.get_soup_from_url(full_url, params=params)

    nav_pages = soup.find("ul", class_="pagination").find_all("li")
    if len(nav_pages) == 1:
        num_pages = 1
    else:
        num_pages = nav_pages[-2].get_text()
        num_pages = int(num_pages)

    # first page
    urls, titles = extract_books_from_soup(soup)

    # rest of the pages
    for i in range(2, num_pages+1):
        params["page"] = i
        soup = await client.get_soup_from_url(full_url, params=params)
        new_urls, new_titles = extract_books_from_soup(soup)

        urls.extend(new_urls)
        titles.extend(new_titles)
    
    if need_close:
        await client.close()

    return urls, titles

async def get_texts(url: str, client=None, book_type="story"):
    need_close = False
    if not client:
        client = AsyncClient()
        need_close = True

    if book_type == "story":
        soup = await client.get_soup_from_url(url + "&chapter=0000")
        num_chapters = soup.find("a", {"title": "Cách tính số chương"}).get_text()
        num_chapters = int(num_chapters)

        if num_chapters == 1:
            full_text = extract_text(soup)
            yield full_text

        else:
            for i in range(1, num_chapters+1):
                chapter_url = f"{url}&chapter={i:04d}"
                soup = await client.get_soup_from_url(chapter_url)

                chapter = [f"Chương {i}"]
                chapter.extend(extract_text(soup))
                yield chapter
    
    elif book_type == "poem":
        soup = await client.get_soup_from_url(url)
        full_text = extract_text(soup)
        yield full_text

    if need_close:
        await client.close()

def extract_text(soup):
    paras = soup.find_all("div", class_=["ms_text", "poem_text"])
    paras = ["".join(x.find_all(text=True)) for x in paras]

    return paras

async def write_book_to_file(url, title, base_dir, client=None, book_type="story", tracker: Tracker=None):
    need_close = False
    if not client:
        client = AsyncClient()
        need_close = True
    
    chapters = get_texts(url, client, book_type=book_type)
    filename = sanitize_vn(title)

    num_chapters = 0
    path = os.path.join(base_dir, f"{filename}.txt")
    with open(path, "w", encoding="utf-8") as f:
        async for paras in chapters:
            num_chapters += 1
            for x in paras:
                f.write(x)
                f.write("\n")
    
    # record books that have been saved
    if tracker:
        tracker.add(title)
        tracker.save()

    if need_close:
        await client.close()
    
    return path, num_chapters

async def write_author_to_file(author, client=None, data_dir="./data", book_type="story"):
    logging.info(f"Collecting books from {author}")

    need_close = False
    if not client:
        client = AsyncClient()
        need_close = True

    # book tracker for each author
    tracker = Tracker(f"{book_type}_{author}_books_tracker", path="trackers")

    folder_name = sanitize_vn(author)
    author_dir = os.path.join(data_dir, book_type, folder_name)
    os.makedirs(author_dir, exist_ok=True)

    urls, titles = await get_books(sanitize_vn(author, delimiter="_"), client=client, book_type=book_type)

    logging.info(f"{len(urls)} books found. Writing them to files")
    
    tasks = []
    num_concurrent = 50
    # parallelize only at book level
    for url, title in zip(urls, titles):
        if tracker and tracker.check(f"{book_type}_{author}_{title}"):
            continue

        task = asyncio.create_task(write_book_to_file(url, title, author_dir, client=client, book_type=book_type, tracker=tracker))
        if len(tasks) > num_concurrent:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        
        tasks.append(task)
    
    books = await asyncio.gather(*tasks)
    paths = [x[0] for x in books]
    num_chapters = [x[1] for x in books]

    if need_close:
        await client.close()
    
    return titles, paths, num_chapters

async def main():
    client = AsyncClient()
    tracker = Tracker("author_tracker", path="trackers")

    authors = {}
    for book_type in ["story", "poem"]:
        with open(f"isach_{book_type}_authors.txt", "r", encoding="utf-8") as f:
            authors[book_type] = [x.rstrip() for x in f]
    
    logging.info(f"List of authors: {authors}", )

    columns=["author", "book_type", "title", "path", "num_chapters"]
    index_csv = CSVWriter("index", columns)
    
    for book_type, book_type_authors in authors.items():
        for auth in book_type_authors:
            id = f"{book_type}_{auth}"
            if tracker.check(id):
                continue
                
            titles, paths, num_chapters = await write_author_to_file(auth, client=client, book_type=book_type)
            tracker.add(id)
            tracker.save()

            num_books = len(titles)
            rows = zip([auth]*num_books, [book_type]*num_books, titles, paths, num_chapters)
            index_csv.write_rows(rows)

    await client.close()

if __name__ == "__main__":
    now = datetime.now()
    logging.basicConfig(
        level=logging.INFO,
        handlers=[
            logging.FileHandler(f"logging_{now.strftime('%Y%m%d_%H%M%S')}.log"),
            logging.StreamHandler()
        ],
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    