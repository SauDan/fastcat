FROM 424075490046.dkr.ecr.ap-east-1.amazonaws.com/alpine:3.20-awscli

RUN apk add bash uuidgen jq

COPY scripts/ /usr/local/bin/

SHELL [ "/bin/bash", "-c" ]
