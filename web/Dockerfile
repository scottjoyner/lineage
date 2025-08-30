FROM nginx:1.27-alpine

# Remove default index
RUN rm -f /usr/share/nginx/html/*

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html /usr/share/nginx/html/index.html
