#!/bin/bash
aws s3 cp target/dewev3-1.0-SNAPSHOT.jar s3://331982-iad/ --region us-east-1
echo https://s3.amazonaws.com/331982-iad/dewev3-1.0-SNAPSHOT.jar
