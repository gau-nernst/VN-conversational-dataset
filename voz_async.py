import os, logging, re, time, pickle
from typing import List
from datetime import datetime
from bs4 import BeautifulSoup
import asyncio, aiohttp


async def get_soup(url: str, session: aiohttp.ClientSession, attempts=3, try_after=30):
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
            f.write("TOPIC ")
            f.write(topic)
            f.write("\n")
            f.write(str(len(threads)))
            f.write("\n")
            for th in threads:
                f.write(th)
                f.write("\n")

    def write_thread_of_posts(self, thread: str, posts: List[str]):
        with open(self.file, "a", encoding="utf-8") as f:
            f.write("THREAD\n")
            f.write(thread)
            f.write("\n")
            for p in posts:
                f.write(p)
                f.write("\n")

class Tracker:
    def __init__(self, path):
        self.path = path
        self.file_path = os.path.join(path, "tracker.pkl")
        self.tracker = {}

        if not os.path.exists(path):
            os.makedirs(path)
        
        if os.path.exists(self.file_path):
            logging.info("Tracker exists on disk. Loading tracker from disk")
            with open(self.file_path, "rb") as f:
                self.tracker = pickle.load(f)

    def save(self):
        with open(self.file_path, "wb") as f:
            pickle.dump(self.tracker, f)


async def get_topics(url: str, session: aiohttp.ClientSession, path, refresh=False):
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
            return None

        topics = []
        titles = soup.find_all("h3", class_="node-title")
        for t in titles:
            links = [f"{url}{x['href']}" for x in t.find_all("a")]
            for l in links:
                try:
                    topic_soup = await get_soup(l, session)
                except aiohttp.ClientResponseError as e:
                    logging.error(e)
                    logging.error(f"Failed to get {l}")
                    continue
                num_pages = get_num_pages(topic_soup)
                topics.append((l, num_pages))
        
        topics.sort(key=lambda x: x[1])
        with open(file_path, "w", encoding="utf-8") as f:
            for t, num_pages in topics:
                f.write(t)
                f.write(" ")
                f.write(str(num_pages))
                f.write("\n")
        
        return topics

def get_num_pages(page_soup: BeautifulSoup):
    nav = page_soup.find("ul", class_="pageNav-main")
    if nav:
        nav_items = [x for x in nav.find_all("li")]
        num_pages = int(nav_items[-1].find("a").get_text())
    else:
        num_pages = 1
    return num_pages

async def get_threads(topic_url: str, host: str, session: aiohttp.ClientSession, threadsWriter: FileWriter, num_pages=None, max_pages: int=2, batch_size: int=50):
    logging.info(f"Started topic {topic_url}")
    if num_pages:
        logging.info(f"{topic_url} has {num_pages} pages")
    threads = []
    topic_name = topic_url.split("/")[-2]

    if not num_pages:
        try:
            topic_soup = await get_soup(topic_url, session)
        except aiohttp.ClientResponseError as e:
            logging.error(e)
            logging.error(f"Error loading topic {topic_name}")
            return None

        num_pages = get_num_pages(topic_soup)

    async def process_page_task(url):
        try:
            page_soup = await get_soup(url, session)
        except aiohttp.ClientResponseError as e:
            logging.error(e)
            return

        links = page_soup.find_all("a", attrs={"data-tp-primary": "on"})
        links = [f"{host}{x['href']}" for x in links]
        threads.extend(links)

    tasks = []
    for page in range(1, min(num_pages, max_pages)+1):
        task = asyncio.create_task(process_page_task(f"{topic_url}page-{page}"))
        tasks.append(task)
        
        if len(tasks) >= batch_size:
            await asyncio.gather(*tasks)

    await asyncio.gather(*tasks)    

    logging.info(f"Finished topic {topic_url}, collected {len(threads)} threads")
    threadsWriter.write_topic_of_threads(topic_name, threads)

    return topic_name, len(threads)

# NOTE: when the thread has a lot of posts → explode in memory
async def get_posts(thread_url: str, topic: str, session: aiohttp.ClientSession, fileWriter: FileWriter=None, count: dict=None, max_pages: int=2, tracker: Tracker=None):
    posts = []

    def process_post(post):
        # remove unwanted elements
        for script in post.find_all("script"):
            script.decompose()
        for reply in post.find_all("blockquote"):
            reply.decompose()
        for link in post.find_all("a"):
            link.decompose()

        # simple cleaning
        post = post.find_all(text=True)
        post = " ".join(post).strip()
        post = re.sub(r"\n+", " ", post)
        post = re.sub(r"\s+", " ", post)
        return post

    try:
        thread_soup = await get_soup(thread_url, session)
    except aiohttp.ClientResponseError as e:
        logging.error(e)
        if e.status == 404:
            logging.error(f"Thread {thread_url} no longer exists")
            if tracker:
                tracker.tracker[topic]["threads"].add(thread_url)
                tracker.save()
        else:
            logging.error(f"Error loading thread {thread_url}")
        return None
    except:
        logging.error(f"Error loading thread {thread_url}")
        return None
    
    # get number of pages for this topic
    num_pages = get_num_pages(thread_soup)

    post_containers = thread_soup.find_all("div", class_="bbWrapper")
    for p in post_containers:
        p = process_post(p)
        if p:
            posts.append(p)

    for page in range(2, min(max_pages, num_pages)+1):
        try:
            page_soup = await get_soup(f"{thread_url}page-{page}", session)
        except aiohttp.ClientResponseError as e:
            logging.error(e)

        post_containers = page_soup.find_all("div", class_="bbWrapper")
        for p in post_containers:
            p = process_post(p)
            if p:
                posts.append(p)

    if fileWriter:
        fileWriter.write_thread_of_posts(thread_url, posts)
    if tracker:
        tracker.tracker[topic]["threads"].add(thread_url)
        tracker.save()
    if count:
        count["posts"] += len(posts)
    return len(posts)

async def write_posts_for_topic(topic, threads, session: aiohttp.ClientSession, path, max_pages=2, tracker: Tracker=None, batch_size: int=50):
    file_path = os.path.join(path, f"{topic}.txt")
    postsWriter = FileWriter(file_path)

    logging.info(f"Started topic {topic} with {len(threads)} threads")
    
    count = {"posts": 0}
    tasks = []
    
    for th in threads:
        if not tracker or th not in tracker.tracker[topic]["threads"]:
            task = asyncio.create_task(get_posts(th, topic, session, fileWriter=postsWriter, count=count, max_pages=max_pages, tracker=tracker))
            tasks.append(task)
            
            # to avoid spawning too many tasks when there are too many threads for the topic
            if len(tasks) >= batch_size:
                await asyncio.gather(*tasks)
                tracker.save()
    
    await asyncio.gather(*tasks)
    tracker.tracker[topic]["finished"] = True
    tracker.save()
    logging.info(f"Finished topic {topic}")
    logging.info(f"Collected {count['posts']} posts for topic {topic}")

    return topic, count["posts"]

async def main_write_all_threads(max_pages=float("inf")):
    host = "https://voz.vn"
    threadsWriter = FileWriter("./voz_async/threads.txt")
    if not os.path.exists("./voz_async/"):
        os.makedirs("./voz_async/")

    time0 = time.time()
    total_threads = 0
    total_topics = 0

    connector = aiohttp.TCPConnector(limit_per_host=20)
    timeout = aiohttp.ClientTimeout(total=0)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        topics = await get_topics(host, session, "./voz_async/")
        
        for t, num_pages in topics:
            _, num_threads = await get_threads(t, host, session, threadsWriter, num_pages=num_pages, max_pages=max_pages)
            total_threads += num_threads
            total_topics += 1
            logging.info(f"Total threads collected: {total_threads}. Total topics processed: {total_topics}")

    logging.info(f"Took {time.time()-time0:.02f}s")

async def main_write_posts(max_pages=float("inf"), max_posts=float("inf")):
    host = "https://voz.vn" 
    threads_file = "./voz_async/threads.txt"
    posts_path = "./voz_async/posts/"
    tracker = Tracker("./voz_async/posts/")

    time0 = time.time()
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
                topic = line.split()[-1]
                num_threads = int(f.readline().rstrip())
                threads = [f.readline().rstrip() for _ in range(num_threads)]
                if topic not in tracker.tracker:
                    tracker.tracker[topic] = {"finished": False, "threads": set()}
                if tracker.tracker[topic]["finished"]:
                    continue
                
                # submit all threads of 1 topic to process
                _, num_posts = await write_posts_for_topic(topic, threads, session, posts_path, max_pages=max_pages, tracker=tracker)

                total_posts += num_posts
                logging.info(f"Total posts collected: {total_posts}")
                if total_posts >= max_posts:
                    break

    logging.info(f"Took {time.time()-time0:.02f}s")

async def test():
    async with aiohttp.ClientSession() as session:
        topics = await get_topics("https://voz.vn", session, "./voz_async50/")
        for t in topics:
            topic_soup = await get_soup(t, session)
            num_pages = get_num_pages(topic_soup)
            print(t, num_pages)

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

    # asyncio.run(main_write_all_threads())
    asyncio.run(main_write_posts())
    # asyncio.run(test())
