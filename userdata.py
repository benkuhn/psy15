from __future__ import division
import json

LIWC_KEYS = [
    "1", #funct@Total function words
    "2", #pronoun@Total pronouns
    "3", #ppron@Personal pronouns
    "4", #i@1st person singular
    "5", #we@1st person plural
    "6", #you@Total 2nd person
    "7", #shehe@3rd person singular
    "8", #they@3rd person plural
    "9", #ipron@Impersonal pronouns
    "10", #article@Articles
    "11", #verb@Common verbs
    "12", #auxverb@Auxiliary verbs
    "13", #past@Past tense
    "14", #present@Present tense
    "15", #future@Future tense
    "16", #adverb@Adverbs
    "17", #preps@Prepositions
    "18", #conj@Conjunctions
    "19", #negate@Negations
    "20", #quant@Quantifiers
    "21", #number@Numbers
    "22", #swear@Swear words
    "121", #social@Social processes
    "122", #family@Family
    "123", #friend@Friends
    "124", #humans@Humans
    "125", #affect@Affective processes
    "126", #posemo@Positive emotion
    "127", #negemo@Negative Emotion
    "128", #anx@Anxiety
    "129", #anger@Anger
    "130", #sad@Sadness
    "131", #cogmech@Cognitive processes
    "132", #insight@Insight
    "133", #cause@Causation
    "134", #discrep@Discrepancy
    "135", #tentat@Tentativeness
    "136", #certain@Certainty
    "137", #inhib@Inhibition
    "138", #incl@Inclusion
    "139", #excl@Exclusion
    "140", #percept@Perceptual processes
    "141", #see@Seeing
    "142", #hear@Hearing
    "143", #feel@Feeling
    "146", #bio@Biological processes
    "147", #body@Body
    "148", #health@Health
    "149", #sexual@Sexual
    "150", #ingest@Ingestion
    "250", #relativ@Relativity
    "251", #motion@Motion
    "252", #space@Space
    "253", #time@Time
    "354", #work@Work
    "355", #achieve@Achievement
    "356", #leisure@Leisure
    "357", #home@Home
    "358", #money@Money
    "359", #relig@Religion
    "360", #death@Death
    "462", #assent@Assent
    "463", #nonfl@Non-fluencies
    "464", #filler@Fillers
    "500", #mentions
    "501", #hashtags
    "502", #total
    ]

tweetfiles = []
userfiles = []
for i in xrange(7):
    tweetfiles.append('data_tweets/part-0000%d' % i)
    userfiles.append('data_users/part-0000%d' % i)

users = {}
nulls = 0
lists = 0
misses = 0
links = 0
partials = 0
profilemissing = 0
liwcmissing = 0

for fname in userfiles:
    print 'loading', fname
    with open(fname) as f:
        for line in f.readlines():
            line = line[:-1]
            uid, blob = line.split('\t')
            # obnoxiously quoted
            uid = uid[1:-1]
            obj = json.loads(blob)
            if obj is None:
                nulls += 1
            elif type(obj) is list:
                lists += 1
            else:
                if not 'age' in obj:
                    #partial user that's not in normal file
                    continue
                obj['follower_ids'] = []
                obj['userid'] = uid
                users[uid] = obj

for uid, user in list(users.iteritems()):
    new_following = []
    for targetid in user['following']:
        links += 1
        targetid = str(targetid)
        if not targetid in users:
            misses += 1
            continue
        target = users[targetid]
        target['follower_ids'].append(uid)
        new_following.append(targetid)
    user['following'] = new_following

print 'bad nulls:', nulls
print 'bad lists:', lists
print 'bad partials:', partials
print 'bad links:', misses, '/', links

for fname in tweetfiles:
    print 'loading', fname
    with open(fname) as f:
        for line in f.readlines():
            line = line[:-1]
            uid, blob = line.split('\t')
            # obnoxiously quoted
            uid = uid[1:-1]
            obj = json.loads(blob)
            total = obj["502"]
            if uid not in users:
                profilemissing += 1
                continue
            user = users[uid]
            user['liwc'] = True
            for key in LIWC_KEYS:
                count = obj.get(key, 0)
                frac = count / total
                user['liwc' + key] = frac

for k in users.keys():
    if not 'liwc' in users[k]:
        liwcmissing += 1
        del users[k]

print 'profile missing:', profilemissing
print 'LIWC missing:', liwcmissing
