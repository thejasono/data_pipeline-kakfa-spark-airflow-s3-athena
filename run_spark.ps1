param(
  [string]$Profile = "AdministratorAccess-251986419027",
  [string]$Region  = "eu-west-2"
)

# 1) Ensure SSO cache is valid (browser opens only if cache expired)
aws sso login --profile $Profile

# 2) Export short-lived creds to a Compose-friendly env file
"AWS_REGION=$Region" | Out-File -Encoding ascii .\.env.aws
aws configure export-credentials --profile $Profile --format env `
| ForEach-Object {
    ($_ -replace '^export\s+', '') -replace '"',''
} | Out-File -Append -Encoding ascii .\.env.aws

# 3) Run Compose with that env file
docker compose --env-file .env.aws up --build
