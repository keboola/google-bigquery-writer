import sys
import traceback
from google_bigquery_writer.exceptions import UserException, ApplicationException
from google_bigquery_writer.app import App
import argparse


def main(args):
    try:
        application = App(data_dir=args.data_dir)
        application.run()
        sys.exit(0)
    except UserException as err:
        message = 'User exception: %s' % err
        print(message, file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    except ApplicationException as err:
        message = 'Application exception: %s' % err
        print(message, file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(2)
    except Exception as err:
        message = 'Unhandled exception: %s' % err
        print(message, file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(2)

if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-d', '--data', dest='data_dir')
    args = argparser.parse_args()
    main(args)
