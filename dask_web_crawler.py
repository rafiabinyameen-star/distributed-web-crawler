import time
import requests
from bs4 import BeautifulSoup
import dask.bag as db
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
import nltk
from nltk.corpus import stopwords
from wordcloud import WordCloud
from multiprocessing import freeze_support

def run_crawler():

    nltk.download('stopwords')
    stop_words = set(stopwords.words('english'))

    def fetch_page(url):
        try:
            response = requests.get(url, timeout=8)
            if response.status_code == 200:
                return response.text
        except:
            return ""
        return ""

    def parse_words(html):
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()
        return [w.lower() for w in text.split() if w.isalpha()]

    urls = [
        'https://byjus.com/cbse/essay-on-computer/',
        'https://www.example.org',
        'https://www.ibm.com/think/topics/parallel-computing'
    ]

    # ---------- Sequential ----------
    start_seq = time.time()
    all_words = []

    for url in urls:
        html = fetch_page(url)
        words = parse_words(html)
        all_words.extend(words)

    seq_counts = Counter([w for w in all_words if w not in stop_words])
    end_seq = time.time()

    print("Sequential Time:", round(end_seq - start_seq, 2), "seconds")
    print("Top 5 words (Sequential):", seq_counts.most_common(5))

    # ---------- Dask Parallel ----------
    start_dask = time.time()

    bag = db.from_sequence(urls)
    word_bag = bag.map(fetch_page).map(parse_words).flatten()
    word_counts = dict(word_bag.frequencies().compute())
    word_counts = {w: c for w, c in word_counts.items() if w not in stop_words}

    end_dask = time.time()

    print("Dask Time:", round(end_dask - start_dask, 2), "seconds")

    # ---------- Visualization ----------
    df = pd.DataFrame(
        sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:20],
        columns=['Word', 'Count']
    )

    plt.figure(figsize=(10,6))
    plt.bar(df['Word'], df['Count'])
    plt.xticks(rotation=45)
    plt.title('Top Words (Dask Parallel)')
    plt.show()

    wc = WordCloud(background_color='white').generate_from_frequencies(word_counts)
    plt.imshow(wc)
    plt.axis('off')
    plt.show()


if __name__ == "__main__":
    freeze_support()
    run_crawler()
