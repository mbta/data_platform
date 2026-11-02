import Config

config :ex_aws,
  # note: 'region' doesn't work. it's inherited from the profile
  # region: [{:system, "AWS_REGION"}, {:awscli, "default", 30}, :instance_role],
  access_key_id: [{:system, "AWS_ACCESS_KEY_ID"}, {:awscli, "default", 30}],
  secret_access_key: [
    {:system, "AWS_SECRET_ACCESS_KEY"},
    {:awscli, "default", 30}
  ]
