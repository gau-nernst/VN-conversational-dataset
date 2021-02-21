import os, logging
import concurrent.futures
from voz import SyncFileWriter, get_threads, write_topics, write_posts_for_topic

MAX_WORKER = 8

def run_write_all_threads():
    host = "https://voz.vn"
    folder = "./voz2"
    write_topics("topics.txt", folder, host)
    
    with open(os.path.join(folder, "topics.txt"), "r", encoding="utf-8") as f:
        topics = [x.rstrip() for x in f]

    total_threads = 0
    threadsWriter = SyncFileWriter(os.path.join(folder, "threads.txt"))
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKER) as executor:
        futures = {}
        for t in topics:
            future = executor.submit(get_threads, topic_url=t, host=host, max_pages=10)
            futures[future] = t.split("/")[-2]
            logging.info(f"Started topic {t}")
        
        for future in concurrent.futures.as_completed(futures):
            threads = future.result()
            total_threads += len(threads)
            threadsWriter.write_topic_of_threads(futures[future], threads)
            logging.info(f"Finished topic {futures[future]}")
            logging.info(f"Obtained {total_threads} threads")


def run_write_posts_by_topics(file):
    host = "https://voz.vn"
    folder = "./voz"
    threads_file = os.path.join(folder, file)
    assert os.path.exists(threads_file)
    
    posts_folder = os.path.join(folder, "posts/")
    if not os.path.exists(posts_folder):
        os.makedirs(posts_folder)

    total_posts = 0
    with open(threads_file, "r", encoding="utf-8") as f:
        topic = f.readline().rstrip().split()[-1]
        threads = []
        for line in f:
            line = line.rstrip()
            if line[:5] == "TOPIC":
                topic, num_posts = write_posts_for_topic(topic, host, threads, posts_folder, max_pages=float("inf"))
                total_posts += num_posts
                topic = line.split()[-1]
                threads = []
            else:
                threads.append(line)
        topic, num_posts = write_posts_for_topic(topic, host, threads, posts_folder, max_pages=float("inf"))
        total_posts += num_posts
        logging.info(f"Obtained a total of {total_posts} posts")
    

if __name__ == "__main__":
    import time
    from datetime import datetime
    
    now = datetime.now()
    logging.basicConfig(
        level=logging.INFO,
        handlers=[
            logging.FileHandler(f"logging_{now.strftime('%Y%m%d_%H%M%S')}"),
            logging.StreamHandler()
        ],
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    
    time0 = time.time()

    run_write_all_threads()
    # run_write_posts_by_topics("threads.txt")

    logging.info(f"Took {time.time()-time0:.0f}s")