"""
Extractor of TF-IDF features
"""
__author__ = "Alisher Mazhirinov"

import re
from collections import Counter

from bs4 import BeautifulSoup
import pandas as pd
from pandas import DataFrame, notnull
import string
import html
from extractor.transformations.base_transformation import Transformation

import langid

english_stopwords = set("""
i me my myself we our ours ourselves you your yours yourself yourselves he him his himself she her hers herself 
it its itself they them their theirs themselves what which who whom this that these those am is are was were be 
been being have has had having do does did doing a an the and but if or because as until while of at by for with 
about against between into through during before after above below to from up down in out on off over under again 
further then once here there when where why how all any both each few more most other some such no nor not only 
own same so than too very s t can will just don should now d ll m o re ve y ain aren couldn didn doesn hadn hasn 
haven isn ma mightn mustn needn shan shouldn wasn weren won wouldn gx tz add one get may feb mar apr may jun jul aug sep oct 
nov dec jan february march april may june july august september october november december january 
one two three four five six seven eight nine ten us ago after before during next last first second third 
fourth fifth sixth seventh eighth ninth tenth top bottom left right center middle up down east west north south 
pm am ams pm pms see day days week weeks month months year years back forward today tomorrow yesterday now later soon 
""".split())

combined_phishing_benign = ['posts', 'profile', 'visit', 'skip', 'report', 'blog', 'content', 'search', 'powered',
                            'well', 'use', 'link', 'business', 'email', 'everything', 'home',
                            'page', 'create', 'website', 'services', 'free', 'data', 'facebook', 'account',
                            'designed', 'things', 'outdated', 'insecure', 'service',
                            'learn', 'information', 'management', 'time', 'health', 'events', 'view',
                            'best', 'find', 'online', 'read', 'standard', 'school']

combined_malware_bening = ['help', 'make', 'time', 'view', 'home', 'products', 'use', 'experience',
                            'information', 'read', 'email', 'services', 'education', 'learn', 'best', 'free',
                            'business', 'service', 'price', 'website', 'data', 'bitcoin', 'investment', 'platform',
                            'financial', 'trading', 'market', 'immediate', 'support', 'management',
                            'health', 'events', 'find', 'online', 'standard', 'search', 'school']


def clean_text(soup):
    for script in soup(["script", "style"]):
        script.decompose()
    text = soup.get_text(separator=" ")
    text = re.sub(r'\s+', ' ', text).strip()
    return text
    

def is_stopword(word):
        return word.lower() in english_stopwords
    
    
def clean_text_advanced(text):
        text = re.sub(r"[^a-zA-Zа-яА-ЯёЁčďěňřšťůžýáíéóúüöäßñç ]", "", text)  # Remove special characters and numbers
        text = re.sub(r"\s+", " ", text).strip()
        words = text.split()  # Break the string into words
        filtered_words = [word for word in words if not is_stopword(word)]  # Eliminate stop words
        return " ".join(filtered_words)

def count_phishing_benign_words(text):
    words = text.lower().split()
    words = [word.strip(string.punctuation) for word in words]

    counter = Counter(words)

    return {f"tfidf_phishing_{word}": counter[word] for word in combined_phishing_benign}

def count_malware_benign_words(text):
    words = text.lower().split()
    words = [word.strip(string.punctuation) for word in words]

    counter = Counter(words)

    return {f"tfidf_malware_{word}": counter[word] for word in combined_malware_bening}

class TFIDFTransformation(Transformation):
    def transform(self, df) -> DataFrame:     
        html_str = " ".join(df["html"].dropna().astype(str))
        
        decoded_html = html.unescape(html_str)

        soup = BeautifulSoup(decoded_html, 'html.parser')

        page_text = clean_text(soup) # Clean the HTML content
        print("Page text:", page_text)
        
        df["html"] = page_text  # Update the DataFrame with cleaned text
        
        cleaned_text_list = [clean_text_advanced(text) for text in df['html'].tolist()]

        df['tfidf_clean_text'] = cleaned_text_list      
        
        lang_confidences = [langid.classify(text) for text in cleaned_text_list]   
        df["ti_lang"] = ["english" if lang == 'en' else "not_english" for lang, _ in lang_confidences]           
        
        if len(cleaned_text_list) > 0 and (df["ti_lang"] == 'english').all():             
            
            df["tfidf_words_count_pb"] = df['tfidf_clean_text'].apply(count_phishing_benign_words)
            word_freq_df = df['tfidf_words_count_pb'].apply(pd.Series)
            df = pd.concat([df, word_freq_df], axis=1)

            df["tfidf_words_count_mb"] = df['tfidf_clean_text'].apply(count_malware_benign_words)
            word_freq_df = df['tfidf_words_count_mb'].apply(pd.Series)
            df = pd.concat([df, word_freq_df], axis=1)
            
            df.drop(columns=['tfidf_clean_text', 'tfidf_words_count_pb', 'tfidf_words_count_mb'], inplace=True)     
            
        return df
    
    
    @property
    def features(self) -> dict[str, str]:
        return {
            "ti_lang": "string"
        }