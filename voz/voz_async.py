import os, logging, re, time, pickle
from typing import List, Tuple, Dict
from datetime import datetime
import bs4
from bs4 import BeautifulSoup
import asyncio, aiohttp


async def get_soup(url: str, session: aiohttp.ClientSession, attempts=3, try_after=30) -> bs4.element.Tag:
    logging.debug(f"Getting {url}")

    for i in range(attempts):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), "lxml")
                    return soup
                else:
                    await response.raise_for_status()
        except aiohttp.ServerDisconnectedError as e:
            logging.error(e)
            logging.error(f"{i+1} attempt(s). Trying again {url} after {try_after}s")
            await asyncio.sleep(try_after)
    
    logging.error(f"Unable to get {url} after {attempts} attempts")


class FileWriter:
    def __init__(self, file: str):
        self.file = file
        
    def write(self, item: str):
        with open(self.file, "a", encoding="utf-8") as f:
            f.write(item)
            f.write("\n")

    def write_list(self, items: List[str]):
        with open(self.file, "a", encoding="utf-8") as f:
            for i in items:
                f.write(i)
                f.write("\n")

    def write_topic_of_threads(self, topic: str, threads: List[str]):
        with open(self.file, "a", encoding="utf-8") as f:
            f.write(f"TOPIC {topic} {len(threads)}\n")
            for th, num_pages in threads:
                f.write(f"{th} {num_pages}\n")

    def write_thread_of_posts(self, thread: str, posts: List[str]):
        with open(self.file, "a", encoding="utf-8") as f:
            f.write(f"THREAD {thread} {len(posts)}\n")
            for p in posts:
                f.write(p)
                f.write("\n")

class Tracker:
    def __init__(self, path, name="tracker"):
        self.file_path = os.path.join(path, f"{name}.txt")
        self.tracker = set()
        self.new_items = []

        if not os.path.exists(path):
            os.makedirs(path)
        
        if os.path.exists(self.file_path):
            logging.info("Tracker exists on disk. Loading tracker from disk")
            with open(self.file_path, "r", encoding="utf-8") as f:
                self.tracker = set([line.rstrip() for line in f])

    def add(self, item: str):
        self.tracker.add(item)
        self.new_items.append(item)

    def check(self, item: str):
        return item in self.tracker

    def save(self):
        with open(self.file_path, "a", encoding="utf-8") as f:
            for item in self.new_items:
                f.write(item)
                f.write("\n")
        self.new_items = []

def process_post(post: bs4.element.Tag, return_list=False) -> str:
    # remove unwanted elements
    for elem in post.find_all(["blockquote", "img", "script", "a"]):
        elem.decompose()

    # simple cleaning
    post = post.find_all(text=True)
    post = [text.strip().strip("\u200b") for text in post if not (text.isspace() or text == "\u200b")]
    post = ["START_POST"] + post
    if return_list:
        return post
    else:
        return "\n".join(post)

def get_num_pages(page_soup: bs4.element.Tag) -> int:
    nav = page_soup.find("ul", class_="pageNav-main")
    if nav:
        nav_items = [x for x in nav.find_all("li")]
        num_pages = int(nav_items[-1].find("a").get_text())
    else:
        num_pages = 1
    return num_pages

async def get_topics(url: str, session: aiohttp.ClientSession, path, refresh=False) -> List[Tuple[str, int]]:
    file_path = os.path.join(path, "topics.txt")
    if not os.path.exists(path):
        os.makedirs(path)
    
    # read from file if file exists and refresh == False
    if os.path.exists(file_path) and not refresh:
        logging.info("Loading topics from file")
        topics = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                topic, num_pages = line.rstrip().split()
                topics.append((topic, int(num_pages)))

        return topics
    
    # scrape from website if file does not exist or refresh == True
    else:
        logging.info("Scraping topics")
        try:
            soup = await get_soup(url, session)
        except aiohttp.ClientResponseError as e:
            logging.error(e)
            logging.error(f"Unable to get topics")
            return None

        topics = []
        titles = soup.find_all("h3", class_="node-title")
        for t in titles:
            links = [x['href'] for x in t.find_all("a")]
            for l in links:
                try:
                    topic_soup = await get_soup(f"{url}{l}", session)
                except aiohttp.ClientResponseError as e:
                    logging.error(e)
                    logging.error(f"Failed to get {l}. Skipping it")
                    continue
                num_pages = get_num_pages(topic_soup)
                topics.append((l, num_pages))
        
        # sort topics by number of pages
        topics.sort(key=lambda x: x[1])
        with open(file_path, "w", encoding="utf-8") as f:
            for t, num_pages in topics:
                f.write(f"{t} {num_pages}\n")
        
        return topics

async def get_threads(topic: str, host: str, session: aiohttp.ClientSession, threadsWriter: FileWriter, num_pages: int, max_pages: int=2, num_concurrent: int=100):
    logging.info(f"Started topic {topic} with {num_pages} pages")
    threads = []

    async def process_page_task(url):
        try:
            page_soup = await get_soup(url, session)
        except aiohttp.ClientResponseError as e:
            logging.error(e)
            logging.error(f"Error loading page {url}. Skipping this page")
            return None
        
        def process_item(thread_item):
            thread = thread_item.find("a", attrs={"data-tp-primary": "on"})["href"]
            pageJump = thread_item.find("span", class_="structItem-pageJump")
            if pageJump:
                for x in pageJump.find_all("a"):
                    thread_pages = int(x.get_text())
            else:
                thread_pages = 1

            return thread, thread_pages

        thread_items = page_soup.find_all("div", class_="structItem-cell--main")
        page_threads = [process_item(thread) for thread in thread_items]
        
        threads.extend(page_threads)

    tasks = []
    for page in range(1, min(num_pages, max_pages)+1):
        task = asyncio.create_task(process_page_task(f"{host}{topic}page-{page}"))

        if len(tasks) > num_concurrent:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        tasks.append(task)

    await asyncio.gather(*tasks)    

    logging.info(f"Finished topic {topic}, collected {len(threads)} threads")
    threadsWriter.write_topic_of_threads(topic, threads)

    return topic, len(threads)


async def get_posts(thread: str, topic: str, host: str, session: aiohttp.ClientSession, fileWriter: FileWriter, num_pages:int, count: dict=None, max_pages: int=2, postTracker: Tracker=None):
    posts = []

    for page in range(1, min(max_pages, num_pages)+1):
        page_url = f"{host}{thread}page-{page}"
        try:
            page_soup = await get_soup(page_url, session)
        except aiohttp.ClientResponseError as e:
            logging.error(e)

            if e.status == 404:
                logging.error(f"Thread {thread} no longer exists. Exiting")
                postTracker.add(thread)
                postTracker.save()
                return 0
            
            else:
                logging.error(f"Unable to load page {page_url}. Skipping this page")
                continue

        post_containers = page_soup.find_all("div", class_="bbWrapper")
        for p in post_containers:
            p = process_post(p)
            if p:
                posts.append(p)

    fileWriter.write_thread_of_posts(thread, posts)
    postTracker.add(thread)
    postTracker.save()
    count["posts"] += len(posts)
    return len(posts)

async def write_posts_for_topic(topic, threads, host, session: aiohttp.ClientSession, path, max_pages=2, postTracker: Tracker=None, num_concurrent: int=100):
    file_path = os.path.join(path, f"{topic.split('/')[-2]}.txt")
    postsWriter = FileWriter(file_path)

    logging.info(f"Started topic {topic} with {len(threads)} threads")
    
    count = {"posts": 0}
    tasks = []
    
    for th, num_pages in threads:
        # check if this thread is collected
        if postTracker.check(th):
            continue

        task = asyncio.create_task(get_posts(th, topic, host, session, postsWriter, num_pages, count=count, max_pages=max_pages, postTracker=postTracker))
            
        if len(tasks) > num_concurrent:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        tasks.append(task)

    await asyncio.gather(*tasks)
    postTracker.save()
    logging.info(f"Finished topic {topic}")
    logging.info(f"Collected {count['posts']} posts for topic {topic}")

    return topic, count["posts"]

async def main_write_all_threads(directory="./data", max_pages=float("inf"), refresh_topics=False):
    host = "https://voz.vn"
    threadsWriter = FileWriter(os.path.join(directory, "threads.txt"))
    threadTracker = Tracker(directory, name="thread_tracker")
    if not os.path.exists(directory):
        os.makedirs(directory)

    total_threads = 0
    total_topics = 0

    connector = aiohttp.TCPConnector(limit_per_host=20)
    timeout = aiohttp.ClientTimeout(total=0)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        topics = await get_topics(host, session, directory, refresh=refresh_topics)
        
        for t, num_pages in topics:
            # there should be a tracker of which topic is finished → to support resume failed operation
            if threadTracker.check(t):
                continue

            _, num_threads = await get_threads(t, host, session, threadsWriter, num_pages=num_pages, max_pages=max_pages)
            
            threadTracker.add(t)
            threadTracker.save()
            total_threads += num_threads
            total_topics += 1
            logging.info(f"Total threads collected: {total_threads}. Total topics processed: {total_topics}")


async def main_write_posts(directory="./data", max_pages=float("inf"), max_posts=float("inf")):
    host = "https://voz.vn"
    threads_file = os.path.join(directory, "threads.txt")
    posts_path = os.path.join(directory, "posts/")
    topicTracker = Tracker(posts_path, name="topic_tracker")
    postTrackers = {}

    total_posts = 0

    connector = aiohttp.TCPConnector(limit_per_host=20)
    timeout = aiohttp.ClientTimeout(total=0)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        with open(threads_file, "r", encoding="utf-8") as f:
            while True:
                # check for end of file condition
                line = f.readline().rstrip()
                if not line:
                    break

                # scrape and write posts for one topic at a time
                _, topic, num_threads = line.split()
                num_threads = int(num_threads)
                threads = [f.readline().rstrip().split() for _ in range(num_threads)]
                threads = [(x[0], int(x[1])) for x in threads]
                
                if topicTracker.check(topic):
                    continue
              
                if topic not in postTrackers:
                    topic_clean = topic.split('/')[-2]
                    postTrackers[topic_clean] = Tracker(posts_path, name=f"{topic_clean}_tracker")

                # submit all threads of 1 topic to process
                _, num_posts = await write_posts_for_topic(topic, threads, host, session, posts_path, max_pages=max_pages, postTracker=postTrackers[topic_clean])
                
                total_posts += num_posts
                topicTracker.add(topic)
                logging.info(f"Total posts collected: {total_posts}")
                if total_posts >= max_posts:
                    break

async def test():
    return

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

    DIRECTORY = "./data_03042021/"
    asyncio.run(main_write_all_threads(directory=DIRECTORY))
    asyncio.run(main_write_posts(directory=DIRECTORY))
    # asyncio.run(test())