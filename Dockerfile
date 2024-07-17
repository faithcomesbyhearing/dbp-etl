FROM public.ecr.aws/docker/library/alpine:latest AS Sofria

WORKDIR /app

RUN apk update
RUN apk add g++ gcc git make musl-dev npm
RUN git clone https://github.com/faithcomesbyhearing/sofria-cli
RUN cd sofria-cli; npm install

FROM public.ecr.aws/docker/library/alpine:latest

WORKDIR /app

RUN apk update && \
    apk add --no-cache ffmpeg jq mysql-client nodejs python3 py3-pip

# --
# To get past the "externally-managed-environment" error
# it looks like there are (at least) two choices:

# Set this environment variable to allow system and eternally managed to coexist
#ENV PIP_BREAK_SYSTEM_PACKAGES 1

# OR create and activate a virtual environment
ENV VIRTUAL_ENV=/app/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN python -m venv $VIRTUAL_ENV
RUN /bin/sh -c "source $VIRTUAL_ENV/bin/activate"
# --

RUN pip install boto3 pymysql pytz awscli

COPY --from=Sofria /app/sofria-cli ./sofria-cli

COPY load load/

COPY docker/run.sh .
CMD ./run.sh
