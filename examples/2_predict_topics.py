import csv
import en_core_web_sm
import gensim
import gzip
import logging
from nltk.corpus import stopwords

# configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class clean_document:
    def __init__(self, input_string, stopwords, nlp):
        self.input_string = input_string
        self.string_lower = self.lower_string()
        self.tokens = self.tokenizer()
        self.tokens_no_stopwords = self.remove_stopwords(stopwords)
        self.annotated = self.annotate(nlp)

    def lower_string(self):
        string_lower = self.input_string.lower()
        return string_lower

    def tokenizer(self):
        tokens = gensim.utils.simple_preprocess(self.string_lower, deacc=False)
        return tokens

    def remove_stopwords(self, stopwords):
        no_stopwords = [line for line in self.tokens if line not in stopwords]
        return no_stopwords

    def annotate(self, nlp):
        doc = nlp(" ".join(self.tokens_no_stopwords))
        new = [token.lemma_ for token in doc if token.pos_ in ["NOUN", "VERB", "ADJ"]]
        return new


# load the spaCy model
nlp = en_core_web_sm.load()
logging.info("Trained pipeline loaded")

# load stopwords
stopwords = set(stopwords.words("english"))
logging.info("Stopwords loaded")

# load responses into a dict
responses = {}
with open("survey_responses.csv") as f:
    for line in list(csv.DictReader(f)):
        responses[line["Respondent WID"]] = {"answer": line["Questionnaire Answer"]}
logging.info(f"{len(responses)} survey responses loaded")

# clean and normalize the survey responses
for wid in responses.keys():
    x = clean_document(responses[wid]["answer"], stopwords, nlp)
    responses[wid]["clean"] = x.annotated
logging.info("Survey responses cleaned and normalized")

# load cleaned comments into a dictionary
id2word = gensim.corpora.Dictionary([responses[wid]["clean"] for wid in responses.keys()])
logging.info("Cleaned responses converted into a Gensim dictionary")

# convert the cleaned documents into a bag-of-words
corpus = [id2word.doc2bow(responses[wid]["clean"]) for wid in responses.keys()]
logging.info("Gensim dictionary converted into a corpus")

# fit LDA model to corpus
model = gensim.models.ldamodel.LdaModel(
    corpus=corpus, num_topics=3, id2word=id2word, random_state=42, chunksize=200, iterations=41, passes=16,
)
logging.info("LDA topic model fit to corpus")

# predict topic for each comment
predictions = []
for wid, text, vec in zip(responses.keys(), [responses[wid]["answer"] for wid in responses.keys()], corpus):
    pred = model[vec]
    stats = {f"Topic {line[0]+1}": line[1] for line in pred}
    row = {"wid": wid, "topic": max(stats, key=stats.get), "topic_score": round(stats[max(stats, key=stats.get)], 4)}
    predictions.append(row)
logging.info("Topics predicted for survey resposnes")

# write predictions to a compressed file
fname = "predictions.csv.gz"
with gzip.open(fname, "wt") as f:
    writer = csv.DictWriter(f, predictions[0].keys())
    writer.writeheader()
    writer.writerows(predictions)
logging.info(f"{fname} created")
