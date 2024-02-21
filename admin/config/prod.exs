import Config

config :admin, Admin.Repo,
  show_sensitive_data_on_connection_error: false,
  ssl: true,
  use_iam_token: true

config :ex_aws,
  # overwrite defaults here, so as to only look at instance role
  access_key_id: :instance_role,
  secret_access_key: :instance_role

config :ehmon, :report_mf, {:ehmon, :info_report}
