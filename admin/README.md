# Admin

An Elixir app to get visibility into the Data Platform pipelines and take action on them.

## Installation

1. Initialize app.
```sh
mix deps.get
```

2. Generate SSL certificates.

```sh
mix x509.gen.selfsigned
```

3. Run the app.
```sh
iex -S mix
```

4. Go to [https://localhost:4000/](https://localhost:4000/)
