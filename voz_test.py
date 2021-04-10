from bs4 import BeautifulSoup
import requests

from voz_async import process_post

def test_process_post():
    test_urls = [
        "https://voz.vn/t/tu-van-cac-tiem-sua-xe-uy-tin-o-sai-gon.193996/",
        "https://voz.vn/t/cung-cap-thung-rac-120lit-thung-rac-cong-cong-gia-re-lh-0911-041-000.262038/"
    ]
    
    for page_url in test_urls:
        response = requests.get(page_url)

        assert response.status_code == 200
        page_soup = BeautifulSoup(response.text, "lxml")

        posts = page_soup.find_all("div", class_="bbWrapper")
        for p in posts:
            p_list = process_post(p, return_list=True)
            # print(p_list)
            assert type(p_list) == list

            p_string = process_post(p)
            assert type(p_string) == str
            assert p_string.startswith("START_POST")
            assert "\u200b" not in p_string
            assert "Click to expand" not in p_string