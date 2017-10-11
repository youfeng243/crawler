
import sys

print '''{
    "type": "object",
    "properties": {'''

for line in sys.stdin:
    linesplit = line.strip().split('\t')
    if linesplit[1].lower() == 'double': linesplit[1] = 'number'
    print '''        "%s": {
            "type": "%s",
            "title": "%s"
        },''' % (linesplit[0], linesplit[1].lower(), linesplit[3])

print '''    }
}'''
