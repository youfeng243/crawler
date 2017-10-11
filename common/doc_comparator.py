import json

def get_doc_none_count(doc):
    return reduce(lambda x, y: x + y, map(lambda x : 1 if doc[x] is None else 0, doc.keys()))

class DocComparator:
    def __init__(self):
        pass

    def is_richer_than(self, former, later):
        # Something is better than nothing
        if former is None:
            return True
        # Default implementation
        key_count_former = len(former)
        key_count_later = len(later)
        if key_count_later > key_count_former:
            # We got a version with more attributes
            return True
        elif key_count_later < key_count_former:
            # Newly merged version has even fewer attributes then the old one, this is a bug
            raise Exception("Newly merged version has even fewer attributes then the old one, former : %s, later : %s",
                            json.dumps(former), json.dumps(later))
        else:
            # Number of attributes is the same, compare comtent
            # TODO: make this configurable if needed
            if later != former:
                return True
            else:
                return False

