import ConfigParser
import os

DIRPATH = os.path.dirname(__file__)

def config_parse(url, str):
    cf = ConfigParser.ConfigParser()
    cf.read(os.path.join(DIRPATH, "conf_source.conf"))
    result = cf.get(url, str)
    return result