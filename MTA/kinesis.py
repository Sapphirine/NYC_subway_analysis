import sys,csv,json

import boto3

sys.path.append('../utils')
import aws


KINESIS_STREAM_NAME = "mtaStream"


def main(fileName):
    print "1"
    # connect to kinesis
    kinesis = aws.getClient('kinesis','us-east-1')
    data = [] # list of dictionaries will be sent to kinesis
    print "2"
    with open(fileName,'rb') as f:
        print "hello"
        dataReader = csv.DictReader(f)
        for row in dataReader:
            print row
            #kinesis.put_record(StreamName=mtaStream, Data=json.dumps(row), PartitionKey='0')
            break
        f.close()




if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Missing arguments"
        sys.exit(-1)
    if len(sys.argv) > 2:
        print "Extra arguments"
        sys.exit(-1)
    try:
        fileName = sys.argv[1]
        main(fileName)
    except Exception as e:
        print e
