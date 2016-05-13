get aws credentials, save it at ../config.txt

pip install boto3
pip install boto


1. run gatherData.py to get enough data
2. run buildTrainingDataSet.py to clean data
3. run creatAMLModel.py to upload dataset to S3, then train a ML model
4. run use_Model to do prediction