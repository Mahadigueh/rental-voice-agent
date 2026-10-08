import re
import requests
from bs4 import BeautifulSoup

LIST_URL = "https://smpdirect.ca/fr/a-louer/?display=list"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "fr-CA,fr;q=0.9",
}

BUILDINGS = [
    ("Complexe Berlioz, Lévis", "https://smpdirect.ca/fr/a-louer/berlioz"),
    ("925 rue Mainguy, Québec", "https://smpdirect.ca/fr/a-louer/925-rue-mainguy"),
    ("3070 chemin Ste-Foy, Québec", "https://smpdirect.ca/fr/a-louer/3070-chemin-ste-foy"),
    ("2810 rue Lamberville, Québec", "https://smpdirect.ca/fr/a-louer/2810-rue-lamberville-quebec-qc"),
    ("2820 rue Lamberville, Québec", "https://smpdirect.ca/fr/a-louer/2820-rue-lamberville-quebec-qc"),
    ("2830 rue Lamberville, Québec", "https://smpdirect.ca/fr/a-louer/2830-rue-lamberville-quebec-qc"),
    ("3360 rue Houde, Trois-Rivières", "https://smpdirect.ca/fr/a-louer/3360-rue-houde-trois-rivieres-qc"),
    ("3220 chemin de la Gare, Québec", "https://smpdirect.ca/fr/a-louer/3220-chemin-de-la-gare-quebec-qc"),
]


def fetch(url):
    response = requests.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status()
    if "One moment, please" in response.text:
        raise RuntimeError(f"Site bloqué pour {url}")
    return response.text


def units(html, building, url):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for link in soup.find_all("a", href=True):
        text = link.get_text(" ", strip=True)
        match = re.search(r"#(\S+)\s*-\s*(.+?)\s*-\s*(\d+)\s*\$", text)
        if not match:
            continue
        rows.append({
            "immeuble": building,
            "unite": match.group(1),
            "type": match.group(2).strip(),
            "prix": int(match.group(3)),
            "url": url,
        })
    return rows


def get_listings():
    listings = []
    for name, url in BUILDINGS:
        listings.extend(units(fetch(url), name, url))
    return listings


if __name__ == "__main__":
    rows = get_listings()
    print(f"{len(rows)} logements\n")
    for row in rows:
        print(f"{row['immeuble']} | #{row['unite']} | {row['type']} | {row['prix']} $/mois")