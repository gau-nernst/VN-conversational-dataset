from bs4 import BeautifulSoup
from urllib.request import urlopen
import os
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import time

# TODO: pass in a driver, don't create a new browser instance
def get_topics(url, driver):
    topics = []

    driver.get(url)
    topics.extend([x.get_attribute("href") for x in driver.find_elements_by_css_selector(".node-title > a")])

    return topics

def get_rooms(topic_url, driver, max_pages=10):
    rooms = []

    driver.get(topic_url)
    rooms.extend([x.get_attribute("href") for x in driver.find_elements_by_css_selector(".structItem-title > a")])
    
    # go to the next page
    count = 1
    try:
        next_button = driver.find_element_by_css_selector(".pageNav-jump--next")
    except:
        next_button = None
    while next_button and count < max_pages:
        next_button.click()
        count += 1

        rooms.extend([x.get_attribute("href") for x in driver.find_elements_by_css_selector(".structItem-title > a")])

        try:
            next_button = driver.find_element_by_css_selector(".pageNav-jump--next")
        except:
            next_button = None
        
    return rooms

def get_texts(room_url, driver, max_pages=10):
    texts = []

    def process_convo(forum_post):
        forum_post = BeautifulSoup(forum_post.get_property("outerHTML"), 'html.parser').div.findAll(text=True, recursive=False)
        forum_post = [sentence.strip() for sentence in forum_post]
        forum_post = '\n'.join(forum_post)
        return forum_post

    driver.get(room_url)
    convo = driver.find_elements_by_css_selector("div.bbWrapper")
    convo = [process_convo(x) for x in convo]
    texts.extend(convo)
    
    # go to next page
    count = 1
    try:
        next_button = driver.find_element_by_css_selector(".pageNav-jump--next")
    except:
        next_button = None
    while next_button and count < max_pages:
        next_button.click()
        count += 1

        convo = driver.find_elements_by_css_selector("div.bbWrapper")
        convo = [process_convo(x) for x in convo]
        texts.extend(convo)

        try:
            next_button = driver.find_element_by_css_selector(".pageNav-jump--next")
        except:
            next_button = None
    
    return texts

if __name__ == "__main__":
    current_dir = os.getcwd()
    geckodriver_path = os.path.join(current_dir, "geckodriver.exe")
    url = "https://voz.vn/"
    time0 = time.time()

    options = Options()
    options.headless = True
    with webdriver.Firefox(options=options, executable_path=geckodriver_path) as driver:
        print("Getting voz.vn topics")
        topics = get_topics(url, driver)
        print("Finished getting all voz.vn topics")
        print(f"Took {time.time()-time0:.0f} seconds")
        time0 = time.time()
        print()

        print("Getting voz.vn rooms")
        rooms = []

        # TODO: multi-threading
        for topic_url in topics[:3]:
            rooms.extend(get_rooms(topic_url, driver, max_pages=1))
            print(f"Rooms scraped: {len(rooms)}", end="\r")
        
        print(f"Rooms scraped: {len(rooms)}")
        print("Finished getting voz.vn rooms")
        print(f"Took {time.time()-time0:.0f} seconds")
        time0 = time.time()
        print()

        print("Writting forum threads to file")
        with open("voz_data.txt", "w", encoding="utf-8") as f:
            lines = 0
            for room_url in rooms:
                texts = get_texts(room_url, driver, max_pages=1)
                for text in texts:
                    lines += 1
                    f.write("NEW POST\n")
                    f.write(text)
                    f.write("\n")
                    print(f"Wrote {lines} lines", end="\r")

        print(f"Wrote {lines} lines")
        print("Finished writting to file")
        print(f"Took {time.time()-time0:.0f} seconds")
        print()