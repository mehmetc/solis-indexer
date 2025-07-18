FROM registry.docker.libis.be/solis/base:latest

ARG VERSION=$VERSION
ARG SERVICE=$SERVICE

WORKDIR /app
USER root

COPY lib lib
COPY run.sh run.sh
COPY data_indexer.rb data_indexer.rb

RUN chown -R solis:solis /app

USER solis:solis

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/app/run.sh"]
# Metadata
LABEL org.opencontainers.image.vendor="KULeuven/LIBIS" \
	org.opencontainers.image.url="https://www.libis.be" \
	org.opencontainers.image.title="SOLIS $SERVICE image" \
	org.opencontainers.image.description="SOLIS $SERVICE image" \
	org.opencontainers.image.version="$VERSION"