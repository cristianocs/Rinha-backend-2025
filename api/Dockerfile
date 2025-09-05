FROM ruby:3.2-slim as builder

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y build-essential libssl-dev libpq-dev

COPY Gemfile Gemfile.lock ./
RUN bundle install --jobs $(nproc) --retry 3

FROM ruby:3.2-slim

WORKDIR /usr/src/app

COPY --from=builder /usr/local/bundle/ /usr/local/bundle/

COPY . .

EXPOSE 3000

CMD ["bundle", "exec", "puma", "-b", "tcp://0.0.0.0:3000", "-w", "2", "-t", "3:3"]