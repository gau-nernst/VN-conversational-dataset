import bs4

from isach import get_soup_from_url, sanitize_vn

def test_get_soup_from_url():
    url = "https://example.com/"
    soup = get_soup_from_url(url)
    assert type(soup) == bs4.BeautifulSoup

    title = soup.find("h1")
    assert title.get_text() == "Example Domain"

def test_sanitize_vn():
    inputs = [
        "luc nao cung the",
        "đi qua hoa cúc",
        "ă â đ ê ô ơ ư",
        "đế quốc ở ngoài bờ sông",
        "Có nên làm thế? - Tập 1: Hồi ức quá khứ;",
        "HiHi------<Olas   "
    ]

    true_outputs = [
        "luc_nao_cung_the",
        "di_qua_hoa_cuc",
        "a_a_d_e_o_o_u",
        "de_quoc_o_ngoai_bo_song",
        "co_nen_lam_the_tap_1_hoi_uc_qua_khu",
        "hihi_olas"
    ]

    outputs = [sanitize_vn(s) for s in inputs]

    for x,y in zip(true_outputs, outputs):
        assert x == y
