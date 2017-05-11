#!/bin/bash
aws kinesis put-record --stream-name dewev3_job --data $1 --partition-key $1 --region us-east-1
