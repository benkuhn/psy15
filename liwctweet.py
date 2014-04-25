from mrjob.job import MRJob
import urllib
import re

fieldsplitter = re.compile(r':\s*')
# include non-terminal apostrophes
wordre = re.compile(r'\w([\w' + "'" + r']*\w)?', re.IGNORECASE)
MENTIONS_KEY = 500
HASHTAGS_KEY = 501
TOTAL_KEY = 502

exact = {}
prefixes = {}
def load_dicts():
    with open('liwcdic2007.dic') as f:
        for line in f.readlines():
            vals = line[:-1].split('\t')
            word = vals[0]
            cats = [int(val) for val in vals[1:] if val != '']
            if word[-1] == '*':
                word = word[:-1]
                prefixes[word] = cats
            else:
                exact[word] = cats

def mycount(s):
    if len(s) == 0:
        return 0
    return s.count(' ') + 1

class LIWCer(MRJob):
    def mapper_init(self):
        load_dicts()
    def mapper(self, _, tweeter):
        path = 'https://s3.amazonaws.com/psy15/tweets/' + tweeter
        req = urllib.urlopen(path)
        text = req.read()
        req.close()
        lines = text.split('\n')
        for line in lines:
            if line.find(':') <= 0:
                continue
            k, v = fieldsplitter.split(line, maxsplit = 1)
            if k == 'Origin':
                words = wordre.findall(v)
                yield (tweeter, {TOTAL_KEY:len(words)})
                for uword in words:
                    word = uword.lower()
                    cats = []
                    # exact match?
                    if word in exact:
                        cats = exact[word]
                    # prefix match?
                    else:
                        for i in xrange(len(word) - 1):
                            sub = word[:i]
                            if sub in prefixes:
                                cats = prefixes[sub]
                                break
                    for cat in cats:
                        yield (tweeter, {cat: 1})
            elif k == 'Hashtags':
                c = mycount(v)
                yield (tweeter, {HASHTAGS_KEY: c})
                yield (tweeter, {TOTAL_KEY: c})
            elif k == 'MentionedEntities':
                c = mycount(v)
                yield (tweeter, {MENTIONS_KEY:c})
                yield (tweeter, {TOTAL_KEY:c})
    def reducer(self, key, values):
        result = {}
        for val in values:
            for k, v in val.iteritems():
                if k in result:
                    result[k] += v
                else:
                    result[k] = v
        yield key, result

if __name__ == '__main__':
    LIWCer.run()
