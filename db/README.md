
### Configuration

We override `env.py` connection to allow for local migrations and RDS migrations. See section marked with `MBTA-specific`. Additionally, we initialize the connection URL in the `__init__.py` as it is utilized by other processes.

### Migrations


