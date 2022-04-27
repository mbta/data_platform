FROM hexpm/elixir:1.13.2-erlang-24.2-alpine-3.13.6 AS builder

# Install Hex+Rebar
RUN mix local.hex --force && \
  mix local.rebar --force

# Install necessary tools
RUN apk --no-cache add git make curl

ENV MIX_ENV=prod

WORKDIR /root

# fetch RDS cert to be used in SSL connection to database
RUN curl https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem \
    -o aws-cert-bundle.pem
# run SHA256 sum check
RUN echo "75f921bbabaeb88e35a0d3e3b4c629a340612decd3918d094f80d2941235c204  aws-cert-bundle.pem" | sha256sum -c -

COPY mix.exs mix.exs
COPY mix.lock mix.lock
COPY config/config.exs config/prod.exs config/prod*.exs config/

RUN mix do deps.get --only prod, deps.compile

COPY lib lib
COPY priv priv

RUN mix compile

COPY config/runtime.exs config/runtime.exs

RUN mix release

# Second stage: copies the files from the builder stage
FROM alpine:3.13.6

RUN apk add --no-cache libstdc++ libgcc libssl1.1 ncurses-libs bash curl dumb-init

# Create non-root user
RUN addgroup -S ingestion && adduser -S -G ingestion ingestion
USER ingestion
WORKDIR /home/ingestion

# Set environment
ENV MIX_ENV=prod TERM=xterm LANG=C.UTF-8

COPY --from=builder --chown=ingestion:ingestion /root/_build/prod/rel /home/ingestion/rel

# copy cert to 'priv' folder where it's referenced by the code
COPY --from=builder --chown=ingestion:ingestion /root/aws-cert-bundle.pem ./priv/aws-cert-bundle.pem

# Ensure SSL support is enabled
RUN /home/ingestion/rel/ex_cubic_ods_ingestion/bin/ex_cubic_ods_ingestion eval ":crypto.supports()"

ENTRYPOINT ["/usr/bin/dumb-init", "--"]

HEALTHCHECK CMD ["/home/ingestion/rel/ex_cubic_ods_ingestion/bin/ex_cubic_ods_ingestion", "rpc", "1 + 1"]
CMD ["/home/ingestion/rel/ex_cubic_ods_ingestion/bin/ex_cubic_ods_ingestion", "start"]
