FROM public.ecr.aws/docker/library/alpine:3.21 AS Sofria

WORKDIR /app

RUN apk update
RUN apk add g++ gcc git make musl-dev npm
RUN git clone https://github.com/faithcomesbyhearing/sofria-cli
RUN cd sofria-cli; npm install

FROM public.ecr.aws/docker/library/alpine:3.21

WORKDIR /app

# Install system dependencies including git and sudo for dev container
RUN apk update && \
    apk add --no-cache \
    ffmpeg \
    jq \
    mysql-client \
    nodejs \
    python3 \
    py3-pip \
    git \
    sudo \
    shadow

# Create vscode user for dev container
ARG USER_ID=1000
ARG GROUP_ID=1000
RUN addgroup -g ${GROUP_ID} vscode && \
    adduser -D -u ${USER_ID} -G vscode -s /bin/sh vscode && \
    echo "vscode ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/vscode && \
    chmod 0440 /etc/sudoers.d/vscode

# --
# To get past the "externally-managed-environment" error
# it looks like there are (at least) two choices:

# Set this environment variable to allow system and externally managed to coexist
#ENV PIP_BREAK_SYSTEM_PACKAGES 1

# OR create and activate a virtual environment
ENV VIRTUAL_ENV=/app/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN python -m venv $VIRTUAL_ENV
RUN /bin/sh -c "source $VIRTUAL_ENV/bin/activate"
# --

# Install Python dependencies from requirements file
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --from=Sofria /app/sofria-cli ./sofria-cli

COPY load load/

COPY docker/run.sh .

# Set ownership of /app directory to vscode user
RUN chown -R vscode:vscode /app

CMD ./run.sh
