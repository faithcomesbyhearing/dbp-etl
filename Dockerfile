FROM public.ecr.aws/docker/library/alpine:latest AS Sofria

WORKDIR /app

RUN apk update
RUN apk add g++ gcc git make musl-dev npm
RUN git clone https://github.com/faithcomesbyhearing/sofria-cli
RUN cd sofria-cli; npm install

FROM public.ecr.aws/docker/library/alpine:latest

WORKDIR /app

RUN apk update
RUN apk add aws-cli ffmpeg jq mysql-client nodejs python3 py3-pip
RUN pip install boto3 pymysql pytz

COPY --from=Sofria /app/sofria-cli ./sofria-cli

COPY load load/

COPY docker/run.sh .
CMD ./run.sh
