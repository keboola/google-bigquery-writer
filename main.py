import sys
import traceback

try:
    print 'test'
except Exception as err:
    print(err, file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(2)

