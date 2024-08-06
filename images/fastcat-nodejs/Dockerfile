FROM public.ecr.aws/docker/library/node:20.16.0-alpine3.20 AS build

COPY nodejs/ /opt/nodejs/

WORKDIR /opt/nodejs
RUN npm install
RUN npm run build
RUN rm -rf src nodejs



FROM public.ecr.aws/docker/library/node:20.16.0-alpine3.20 AS final

RUN apk add bash uuidgen jq aws-cli

COPY --from=build /opt/nodejs /opt/nodejs
ENV NODE_ENV=production
WORKDIR /opt/nodejs
RUN npm install

COPY scripts/* /usr/local/bin/

WORKDIR /tmp
SHELL [ "/bin/bash", "-c" ]


#end
