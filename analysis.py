from __future__ import division
from datetime import datetime
import userdata

def print_projection(fname, columns):
    with open(fname, 'w') as outfile:
        buf = ['ID']
        for k in columns.keys():
            buf.append(',')
            buf.append(k)
        buf.append('\n')
        def flush_buf():
            outfile.write(''.join(buf))
            buf[:] = []
        for uid, user in userdata.users.iteritems():
            buf.append(uid)
            for projname, projection in columns.iteritems():
                buf.append(',')
                buf.append(projection(user))
            buf.append('\n')
            if len(buf) > 10000:
                flush_buf()
        flush_buf()

def getter(attr):
    return lambda (user): str(user[attr])

def follower_average(attr):
    def projection(user):
        lst = [userdata.users[followerid][attr] for followerid in user['follower_ids'] if followerid in userdata.users]
        if len(lst) == 0:
            return ''
        else:
            return str(sum(lst) / len(lst))
    return projection

def following_average(attr):
    def projection(user):
        lst = [userdata.users[followerid][attr] for followerid in user['following'] if followerid in userdata.users]
        if len(lst) == 0:
            return ''
        else:
            return str(sum(lst) / len(lst))
    return projection

def parse_age(age):
    # I don't know if the date format got messed up in the MapReduce or what, but holy mackerel
    try:
        tmp = age[:-8] + age[-4:]
        return datetime.strptime(tmp, '%a %b %d %H:%M:%S %Y')
    except:
        tmp = age[:-4]
        return datetime.strptime(tmp, '%d %b %Y %H:%M:%S')

now = datetime.now()
def get_activity(user):
    start = parse_age(user['age'])
    time_active = (now - start).days
    tweets = int(user['statuses'])
    return str(tweets / time_active)

cols = {
    'funct': getter('liwc1'),
    'friend_funct': following_average('liwc1'),
    'follow_funct': follower_average('liwc1'),
    'pronoun': getter('liwc2'),
    'friend_pronoun': following_average('liwc2'),
    'follow_pronoun': follower_average('liwc2'),
    'ppron': getter('liwc3'),
    'friend_ppron': following_average('liwc3'),
    'follow_ppron': follower_average('liwc3'),
    'i': getter('liwc4'),
    'friend_i': following_average('liwc4'),
    'follow_i': follower_average('liwc4'),
    'we': getter('liwc5'),
    'friend_we': following_average('liwc5'),
    'follow_we': follower_average('liwc5'),
    'you': getter('liwc6'),
    'friend_you': following_average('liwc6'),
    'follow_you': follower_average('liwc6'),
    'shehe': getter('liwc7'),
    'friend_shehe': following_average('liwc7'),
    'follow_shehe': follower_average('liwc7'),
    'they': getter('liwc8'),
    'friend_they': following_average('liwc8'),
    'follow_they': follower_average('liwc8'),
    'swear': getter('liwc22'),
    'friend_swear': following_average('liwc22'),
    'follow_swear': follower_average('liwc22'),
    'social': getter('liwc121'),
    'friend_social': following_average('liwc121'),
    'follow_social': follower_average('liwc121'),
    'friend': getter('liwc123'),
    'friend_friend': following_average('liwc123'),
    'follow_friend': follower_average('liwc123'),
    'humans': getter('liwc124'),
    'friend_humans': following_average('liwc124'),
    'follow_humans': follower_average('liwc124'),
    'affect': getter('liwc125'),
    'friend_affect': following_average('liwc125'),
    'follow_affect': follower_average('liwc125'),
    'posemo': getter('liwc126'),
    'friend_posemo': following_average('liwc126'),
    'follow_posemo': follower_average('liwc126'),
    'negemo': getter('liwc127'),
    'friend_negemo': following_average('liwc127'),
    'follow_negemo': follower_average('liwc127'),
    'friends': getter('friends'),
    'followers': getter('followers'),
    'tweets': getter('statuses'),
    'activity': get_activity,
}

def dump():
    print_projection('data.csv', cols)
