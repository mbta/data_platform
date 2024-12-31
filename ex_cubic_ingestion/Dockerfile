FROM hexpm/elixir:1.18.1-erlang-27.1.3-alpine-3.21.0 AS builder

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
RUN echo "390fdc813e2e58ec5a0def8ce6422b83d75032899167052ab981d8e1b3b14ff2  priv/aws-cert-bundle.pem" | sha256sum -c -

RUN mix release

# Second stage: copies the files from the builder stage
FROM alpine:3.21.0

RUN apk add --no-cache libssl3 dumb-init libstdc++ libgcc ncurses-libs

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
