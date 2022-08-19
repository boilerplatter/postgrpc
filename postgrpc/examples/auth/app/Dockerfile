FROM node:latest

# set up environment variables
ARG REACT_APP_KRATOS_PUBLIC_URL
ARG REACT_APP_POSTGRPC_URL
ENV NODE_ENV production

# set up working directory
RUN mkdir -p /app
WORKDIR /app

# install dependencies
COPY ./package-lock.json /app/package-lock.json
COPY ./package.json /app/package.json
RUN npm install --force # FIXME: no forcing!
RUN npm install -g serve

# bundle the assets
COPY ./src /app/src
COPY ./public /app/public
RUN npm run build

# run the app
CMD serve -s build -p $PORT
