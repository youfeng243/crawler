import re

from i_util.tools import get_md5

HZPOST_PATTERN = re.compile(ur"HZPOST=([A-Za-z0-9+%/=])*")

def short_url(url):
    return HZPOST_PATTERN.sub("HZPOST={}".format(get_md5(url)), url)[:512]


if __name__ == "__main__":
    ss = "http://www.landchina.com/default.aspx?tabid=263&HZPOST=eyJUQUJfUXVlcnlDb25kaXRpb25JdGVtI#&a=ber"
    print short_url(ss)