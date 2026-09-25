FROM elixir:1.20.4-alpine AS build
RUN mix local.hex --force && mix local.rebar --force
ENV MIX_ENV=prod
WORKDIR /sink
COPY mix.exs ./
COPY lib ./lib
RUN mix deps.get --only prod && mix release sink

FROM elixir:1.20.4-alpine
COPY --from=build /sink/_build/prod/rel/sink /app
ENV RELEASE_DISTRIBUTION=none
EXPOSE 4200
CMD ["/app/bin/sink", "start"]
