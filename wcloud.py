import re

from wordcloud import WordCloud
import matplotlib.pyplot as plt
import pandas as pd

from janome.tokenizer import Tokenizer


FONT_PATH="/usr/share/fonts/opentype/ipaexfont-gothic/ipaexg.ttf"

INFILE="resrep_FY2019-2023.csv"
DATA="consignee"  # or "consignor", "consignor.1st", "title"

df = pd.read_csv(INFILE, encoding="utf-8")
print(df.columns)

def to_frequencies(words):
    freqs = {}
    for w in words:
        if w in freqs:
            n = freqs[w]
            n += 1
            freqs[w] = n
        else:
            freqs[w] = 1
    return freqs

def ja_words(texts):
    tknzr = Tokenizer()
    words = []
    for text in texts:
        for tok in tknzr.tokenize(text):
            tokinfo = re.split(r"\t|,", str(tok))
            if tokinfo[1] == "名詞" and tokinfo[2] in ("一般", "固有名詞"):
                words.append(tokinfo[0])
    return words

data = DATA

if data == "consignee":
    words = [x for x in df.consignee]
elif data == "consignor":
    words = [x for x in df.consignor]
elif data == "consignor.1st":
    words = [x for x in df.consignor]
    words = [x.split(":")[0] for x in words]
elif data == "title":
    words = ja_words([x for x in df.title])
    excludes = "令和 年度 経済 産業 事業".split(" ")
    words = [w for w in words if w not in excludes]

freqs = to_frequencies(words)

wordcloud = WordCloud(width=800, height=600, 
                      background_color="black", 
                      colormap="summer",
                      #colormap="Set3",
                      #colormap="cividis",
                      #colormap="Spectral", #"summer", #"cividis",
                      collocations=False,  # to avoid words output twice
                      font_path=FONT_PATH,
                      min_font_size=10).generate_from_frequencies(freqs)

#plt.imshow(wordcloud)
#plt.show()
wordcloud.to_file("wc.png")
print(wordcloud.words_)
