# Dashboard image: build with Node, serve with unprivileged nginx.
# Build context: web/.
#   docker build -t clearline-web:dev web

FROM --platform=$BUILDPLATFORM node:24.22.3-trixie-slim AS build
WORKDIR /web
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY . .
RUN npm run build

# Static files are architecture independent, so only this stage is per platform.
FROM nginxinc/nginx-unprivileged:1.30.6-alpine
ENV CLEARLINE_GATEWAY_UPSTREAM=gateway:8080 \
    CLEARLINE_BALANCE_UPSTREAM=balance:8081 \
    CLEARLINE_RECONCILER_UPSTREAM=reconciler:8082 \
    NGINX_ENVSUBST_FILTER=^CLEARLINE_
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /web/dist /usr/share/nginx/html
EXPOSE 8080
