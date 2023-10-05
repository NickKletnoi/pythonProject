
import spacy
nlp = spacy.load('en_core_web_sm')
doc = nlp(u'Tesla is looking at buying U.S. startup for $6 million')

mystring = '"We\'re moving to L.A.!"'

# for token in doc:
#     print(token.text, token.pos_, token.dep_)

#nlp.pipeline
#tagger, parser, ner

doc = nlp(mystring)

# for token in doc:
#     #print(token, token.pos_, token.dep_)
#     print(token)

#print(doc[3:6])

doc8 = nlp(u"Apple to build a Hong Kong factory for $6 million")

# for token in doc8:
#     print(token.text, end=' | ')


# for entity in doc8.ents:
#     print(entity)
#     print(entity.label_)
#     print(str(spacy.explain(entity.label_)))
#     print('\n')


doc9 = nlp(u"Autonomous cars shift insurance liability toward manufacturers.")

for chunk in doc9.noun_chunks:
    print(chunk.text)


from spacy import displacy

doc = nlp(u"Apple is going to build a U.K. factory for $6 million.")

#displacy.render(doc, style='dep', jupyter=False, options={'distance': 110})
#displacy.serve(doc, style='dep')

### stemming #######


from nltk.stem.porter import PorterStemmer

p_stemmer = PorterStemmer()
words = ['run', 'runner', 'ran', 'runs', 'easily', 'fairly']

# for word in words:
#     print(word + '------->' + p_stemmer.stem(word))

from nltk.stem.snowball import SnowballStemmer

s_stemmer = SnowballStemmer(language='english')

words = ['run', 'runner', 'ran', 'runs', 'easily', 'fairly', 'fairness','generous', 'generation', 'generate']

# for word in words:
#     print(word + '------->' + s_stemmer.stem(word))


##### Lemmitization #####

# for word in words:
#     print(word + '------->' + s_stemmer.stem(word))

doc10 = nlp(u"I am a runner running in a race because I love to run since I ran today")

for token in doc10:
    print(f'{token.text:{10}} {token.pos_:{8}} {token.lemma:<{22}} {token.lemma_}')
