#!/usr/bin/env bash
cd /app

if [ -z "$SERVICE_ROLE" ]; then
  echo "Please set a SERVICE_ROLE environment variable"
  exit 255
fi

case $SERVICE_ROLE in
indexer)
  bundle exec ruby data_indexer.rb
  ;;
*)
  echo "Unknown SERVICE_ROLE environment variable"
  exit 254
  ;;
esac

