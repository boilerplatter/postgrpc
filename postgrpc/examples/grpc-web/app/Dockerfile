FROM node:latest

# set up externally-configured environment variables
ARG POSTGRPC_URL

# set up working directory
RUN mkdir -p /examples/grpc-web/app
RUN mkdir -p /proto
WORKDIR /examples/grpc-web/app

# copy the protobuf files
COPY ./proto/ /proto/

# install dependencies
COPY ./examples/grpc-web/app/package-lock.json ./package-lock.json
COPY ./examples/grpc-web/app/package.json ./package.json
RUN npm install
RUN npm install -g serve

# bundle the assets
COPY ./examples/grpc-web/app/index.* ./
COPY ./examples/grpc-web/app/proto.js ./
COPY ./examples/grpc-web/app/clients/* ./clients/
ENV NODE_ENV production
RUN npm run build

# run the app
CMD serve -s dist -p $PORT
