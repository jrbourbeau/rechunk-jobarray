#!/bin/bash
coiled batch run ./run_rechunk.sh  \
	--arm \
	--vm-type "c7g.8xlarge" \
	--secret-env AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
	--secret-env AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY  
