include ../boilerplate.mk

# DEV
# ECR_URL := 078432969830.dkr.ecr.us-west-2.amazonaws.com/dbp-etl-dev-zrg0q2rhv7shv7hr

# NEWDATA
# ECR_URL := 078432969830.dkr.ecr.us-west-2.amazonaws.com/dbp-etl-newdata-fiu49s0cnup1yr0q

.PHONY: image
image:
> aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin $(ECR_URL)
> docker build . -t dbp-etl
> docker tag dbp-etl:latest $(ECR_URL):latest
> docker push $(ECR_URL):latest
