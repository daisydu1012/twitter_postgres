#!/bin/sh

files='
test-data.zip
'

echo 'load normalized'
for file in $files; do
    python3 load_tweets.py \
        --db postgresql://postgres:pass@localhost:25431/postgres \
        --inputs "$file"
done

echo 'load denormalized'
for file in $files; do
    unzip -p "$file" | sed 's/\\u0000//g' | psql postgresql://postgres:pass@localhost:25432/postgres -c "COPY tweets_jsonb(data) FROM STDIN csv quote e'\x01' delimiter e'\x02'"
done
