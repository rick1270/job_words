"""Spacy nlp functions"""

from itertools import chain
import numpy as np
import spacy
import pandas as pd
from spacy.matcher import Matcher
nlp = spacy.load("en_core_web_lg")
stopwords = list(nlp.Defaults.stop_words)

def spacy_proper(doc):
    """Takes string as input and returns a list of Proper Nouns"""
    pn_list = []
    for tok in doc:
        if tok.pos_ == 'PROPN':
            pn_list.append(tok.text)
        else:
            pass
    return pn_list

def sentence_parse_proper(sentences):
    """Parses list of sentences and returns list of proper nouns"""
    col_lists = []
    try:
        for sentence in sentences:
            ss= sentence.strip()
            doc = nlp(ss)
            pn = spacy_proper(doc)
            col_lists.append(pn)
    except ValueError:
        col_lists.append([])
    return set(chain.from_iterable(col_lists))

def pattern_lower(csv_file, column_name):
    """Formats column from csv file into patterns for matching"""
    pattern_list = []
    df = pd.read_csv(csv_file)
    word_list = list(df[column_name])
    clean_word_list = [str(t).lower().strip() for t in word_list if t is not np.nan]
    split_list = [t.split() for t in clean_word_list ]
    for i in range(len(split_list)):
        words = []
        sentence = split_list[i]
        for w in sentence:
            pattern = dict(LOWER = str(w))
            words.append(pattern)
        pattern_list.append(words)
    # print(f'{column_name} now contains {len(pattern_list)} keywords')
    return pattern_list


def data_word_match(sentence, csv_file, column_name):
    """Matches phrase patterns"""
    matcher=Matcher(nlp.vocab)
    word_patterns = pattern_lower(csv_file, column_name)
    matcher.add(column_name, word_patterns, greedy='FIRST')
    doc = nlp(sentence)
    matches = matcher(doc)
    words = []
    for start, end in matches:  ##match_id, not used
        span = doc[start:end]
        words.append(span.text)
    return list(words)

# TODO: use lemmatization to match base words instead of exact # [fixme]

def sentence_parse_data_words (sentences, csv_file, column_name):
    """Takes string as input and returns a list of Proper Nouns"""
    words_lists = []
    try:
        for sentence in sentences:
            words = data_word_match(sentence,csv_file, column_name)
            words_lists.append(words)
    except ValueError:
        words_lists.append([])
    return set(chain.from_iterable(words_lists))
