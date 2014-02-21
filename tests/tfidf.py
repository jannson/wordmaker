# -*- coding=utf-8 -*-

import sys, re, codecs
from yaha import DICTS, get_dict
from yaha.analyse import idf_freq

median_idf = sorted(idf_freq.values())[len(idf_freq)/4]

def sort_with_tfidf(freq_file_name):
    stopwords = get_dict(DICTS.STOPWORD)

    old_freqs = {}
    total = 0
    st_list = None
    with codecs.open(freq_file_name, "r", "utf-8") as file:
        for line in file:
            ws = line.split()
            if stopwords.has_key(ws[0]):
                continue

            if len(ws) >= 2:
                f = int(ws[1])
                old_freqs[ws[0]] = f
                total += f

        freqs = [(k,v/total) for k,v in old_freqs.iteritems()]
        tf_idf_list = [(v * idf_freq.get(k,median_idf),k) for k,v in freqs]
        st_list = sorted(tf_idf_list, reverse=True)
    
    with codecs.open(freq_file_name+".o", "w", "utf-8") as file:
        for v, w in st_list:
            file.write("%s %d\n" % (w, old_freqs[w]))

sort_with_tfidf("sogo_utf.txt")
