FROM hexpm/elixir:1.13.2-erlang-24.2-alpine-3.13.6 AS builder

# Install Hex+Rebar
RUN mix local.hex --force && \
  mix local.rebar --force

# Install necessary tools
RUN apk --no-cache add git make curl

ENV MIX_ENV=prod

WORKDIR /root

COPY mix.exs mix.exs
COPY mix.lock mix.lock
COPY config/config.exs config/prod.exs config/prod*.exs config/

RUN mix do deps.get --only prod, deps.compile

COPY lib lib
COPY priv priv

RUN mix compile

COPY config/runtime.exs config/runtime.exs

# fetch RDS cert to be used in SSL connection to database
RUN curl https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem \
    -o priv/aws-cert-bundle.pem
RUN sha256sum priv/aws-cert-bundle.pem
# run SHA256 sum check
RUN echo "976de749e16140c2e91e2ea1daa508ef716be5decafbb9f0f77087abc879487b  priv/aws-cert-bundle.pem" | sha256sum -c -

RUN mix release

# Second stage: copies the files from the builder stage
FROM alpine:3.13.6

RUN apk add --no-cache libstdc++ libgcc libssl1.1 ncurses-libs bash curl dumb-init

# Create non-root user
RUN addgroup -S ex_cubic_ingestion_app && adduser -S -G ex_cubic_ingestion_app ex_cubic_ingestion_app
USER ex_cubic_ingestion_app
WORKDIR /home/ex_cubic_ingestion_app

# Set environment
ENV MIX_ENV=prod TERM=xterm LANG=C.UTF-8

COPY --from=builder --chown=ex_cubic_ingestion_app:ex_cubic_ingestion_app /root/_build/prod/rel /home/ex_cubic_ingestion_app/rel

# Ensure SSL support is enabled
RUN /home/ex_cubic_ingestion_app/rel/ex_cubic_ingestion/bin/ex_cubic_ingestion eval ":crypto.supports()"

ENTRYPOINT ["/usr/bin/dumb-init", "--"]

HEALTHCHECK CMD ["/home/ex_cubic_ingestion_app/rel/ex_cubic_ingestion/bin/ex_cubic_ingestion", "rpc", "1 + 1"]
CMD ["/home/ex_cubic_ingestion_app/rel/ex_cubic_ingestion/bin/ex_cubic_ingestion", "start"]
