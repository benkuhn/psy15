from mrjob.job import MRJob

class Filterer(MRJob):
    def mapper_init(self):
        self.tweeters = set()
        print 'reading tweeters'
        with open('tweeters.txt') as f:
            for line in f.readlines():
                self.tweeters.add(line[:-1])
        print 'done!'
    def mapper(self, uid, val):
        vals = val.split('\t')
        if len(vals) <= 1:
            return
        uid = vals[0]
        cols = vals[1:]
        if uid in self.tweeters:
            if len(cols) > 1:
                result = {}
                result['username'] = cols[0]
                result['friends'] = cols[1]
                result['followers'] = cols[2]
                result['statuses'] = cols[3]
                result['favorites'] = cols[4]
                result['age'] = cols[5]
                result['location'] = cols[6]
                yield (uid, result)
            else:
                otherid = cols[0]
                print uid, otherid
                if otherid in self.tweeters:
                    yield (uid, { 'following':[otherid] })
    def reducer(self, uid, vals):
        result = { 'following':[] }
        for val in vals:
            for k, v in val.iteritems():
                if k == 'following':
                    result[k].extend(v)
                else:
                    result[k] = v
        yield (uid, result)

if __name__ == '__main__':
    Filterer.run()
