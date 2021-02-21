import os, requests, logging, re, time
from typing import List
from requests.models import HTTPError, Request
from bs4 import BeautifulSoup
import threading, concurrent.futures

MAX_WORKER = 8

def get_soup(url: str):
    logging.debug(f"Getting {url}")
    local_thread = threading.local()
    if not hasattr(local_thread, "session"):
        local_thread.session = requests.Session()

    response = local_thread.session.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "lxml")
        return soup
    else:
        response.raise_for_status()

class SyncFileWriter:
    def __init__(self, file: str):
        self.file = file
        self.lock = threading.Lock()
    
    def write(self, item: str):
        with self.lock:
            with open(self.file, "a", encoding="utf-8") as f:
                f.write(item)
                f.write("\n")

    def write_list(self, items: List[str]):
        with self.lock:
            
            logging.debug(f"Writing to {self.file}")
            with open(self.file, "a", encoding="utf-8") as f:
                for i in items:
                    f.write(i)
                    f.write("\n")
            

    def write_topic_of_threads(self, topic: str, threads: List[str]):
        with self.lock:
            logging.debug(f"Writing to {self.file}")
            with open(self.file, "a", encoding="utf-8") as f:
                f.write("TOPIC ")
                f.write(topic)
                f.write("\n")
                for th in threads:
                    f.write(th)
                    f.write("\n")

    def write_thread_of_posts(self, thread: str, posts: List[str]):
        with self.lock:
            logging.debug(f"Writing to {self.file}")
            with open(self.file, "a", encoding="utf-8") as f:
                f.write("THREAD\n")
                f.write(thread)
                f.write("\n")
                for p in posts:
                    f.write(p)
                    f.write("\n")

def get_topics(url: str):
    try:
        soup = get_soup(url)
    except HTTPError as e:
        logging.error(e)
        return None

    topics = []
    titles = soup.find_all("h3", class_="node-title")
    for t in titles:
        links = [f"{url}{x['href']}" for x in t.find_all("a")]
        topics.extend(links)
    
    return topics

def get_threads(topic_url: str, host: str, max_pages: int=2):
    threads = []
    count = 0
    url = topic_url

    while True:
        count += 1
        try:
            topic_soup = get_soup(url)
        except HTTPError as e:
            logging.error(e)
            break

        links = topic_soup.find_all("a", attrs={"data-tp-primary": "on"})
        links = [f"{host}{x['href']}" for x in links]
        threads.extend(links)
        
        next_button = topic_soup.find("a", class_="pageNav-jump--next")
        if next_button and count < max_pages:
            url = f"{host}{next_button['href']}"
        else:
            break
 
    return threads

def get_posts(thread_url: str, host: str, max_pages: int=2):
    posts = []
    count = 0
    url = thread_url

    def process_post(post):
        for script in post.find_all("script"):
            script.decompose()
        for reply in post.find_all("blockquote"):
            reply.decompose()
        for link in post.find_all("a"):
            link.decompose()

        post = post.find_all(text=True)
        post = " ".join(post).strip()
        post = re.sub(r"\n+", " ", post)
        post = re.sub(r"\s+", " ", post)
        return post

    while True:
        count += 1
        try:
            thread_soup = get_soup(url)
        except HTTPError as e:
            logging.error(e)
            break
        
        post_containers = thread_soup.find_all("div", class_="bbWrapper")
        for p in post_containers:
            p = process_post(p)
            if p:
                posts.append(p)

        next_button = thread_soup.find("a", class_="pageNav-jump--next")
        if next_button and count < max_pages:
            url = f"{host}{next_button['href']}"
        else:
            break
    
    return posts

def write_topics(filename, dir, host, force=False):
    if not os.path.exists(dir):
        os.makedirs(dir)
    
    file_path = os.path.join(dir, filename)
    if not os.path.exists(file_path) or force:
        topics = get_topics(host)
        with open(file_path, "w", encoding="utf-8") as f:
            for t in topics:
                f.write(t)
                f.write("\n")
    

def write_posts_for_topic(topic, host, threads, folder, max_pages=2):
    file_path = os.path.join(folder, f"{topic}.txt")
    postsWriter = SyncFileWriter(file_path)

    num_posts = 0
    logging.info(f"Started topic {topic}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKER) as executor:
        futures = {}
        for th in threads:
            future = executor.submit(get_posts, thread_url=th, host=host, max_pages=max_pages)
            futures[future] = th

        for future in concurrent.futures.as_completed(futures):
            posts = future.result()
            num_posts += len(posts)
            postsWriter.write_thread_of_posts(futures[future], posts)
    
    logging.info(f"Finished topic {topic}")
    logging.info(f"Obtained {num_posts} posts for topic {topic}")

    return topic, num_posts


