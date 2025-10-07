# AWS CLI SSO Profile Usage

This project can interact with AWS resources during local development. If you are using AWS IAM Identity Center (formerly AWS SSO), use the following workflow to authenticate the AWS CLI without storing long-lived access keys.

## 1. Install the AWS CLI
Ensure that the AWS CLI v2 is installed on your machine. On Windows, you can use `winget install AWS.AWSCLI`.

Verify the installation:

```powershell
aws --version
```

## 2. Configure an SSO profile
Create or edit the AWS CLI config file at `%USERPROFILE%\.aws\config` (Windows) or `~/.aws/config` (macOS/Linux) to include your SSO profile.

```ini
[profile AdministratorAccess-251986419027]
sso_session = project-1-sso
sso_account_id = 251986419027
sso_role_name = AdministratorAccess
region = eu-west-2
output = json

[sso-session project-1-sso]
sso_start_url = https://d-9c675e8e1c.awsapps.com/start/#
sso_region = eu-west-2
sso_registration_scopes = sso:account:access
```

> **Tip:** You can generate this structure with `aws configure sso` to avoid editing the file manually.

## 3. Log in with SSO
Use the AWS CLI to establish a session for the configured profile **before** running
any tooling (such as `docker compose`) that depends on AWS access:

```powershell
aws sso login --profile AdministratorAccess-251986419027
```

This command opens a browser window where you sign in to your Identity Center. After approval, the CLI caches temporary credentials locally. The cache is then reused by subsequent CLI calls from the same machine.

If you want a single command that both refreshes the SSO session and starts your local
stack, you can chain the commands:

```powershell
aws sso login --profile AdministratorAccess-251986419027 `
  ; docker compose up -d --build
```

The `aws sso login` command completes quickly when the cached session is still valid.
If the session has expired, it will prompt for re-authentication before continuing to
`docker compose`.

## Running the application

Follow these steps every time you want to bring the stack online with temporary SSO
credentials. No additional system configuration is required—the Compose file already
consumes `.env` and `.env.aws` automatically.

1. **Log in to AWS SSO**

   ```powershell
   aws sso login --profile AdministratorAccess-251986419027
   ```

2. **Export fresh credentials for Docker Compose**

   Replace the placeholders in `.env.aws` with the temporary keys produced by the CLI.
   Pick the command that matches your shell:

   - **Windows PowerShell**

     ```powershell
     "AWS_REGION=eu-west-2" | Set-Content -Encoding ascii .\.env.aws
     aws configure export-credentials --profile AdministratorAccess-251986419027 --format env `
     | ForEach-Object { ($_ -replace '^export\s+', '') -replace '"','' } `
     | Add-Content -Encoding ascii .\.env.aws
     ```

   - **macOS/Linux (bash/zsh)**

     ```bash
     aws configure export-credentials --profile AdministratorAccess-251986419027 --format env \
       | sed -e 's/^export //' -e 's/"//g' > .env.aws
     echo "AWS_REGION=eu-west-2" >> .env.aws
     ```

   The resulting `.env.aws` file contains `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
   `AWS_SESSION_TOKEN`, and `AWS_REGION` values that the Spark containers read on startup.

3. **Start the stack**

   ```powershell
   docker compose up -d --build
   ```

   The services pick up the credentials from `.env.aws`, so there is nothing else you need
   to change before the pipeline comes online.

## 4. Use the profile without environment variables
With an SSO profile, you **do not** need to set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, or `AWS_SESSION_TOKEN` environment variables. Instead, pass the profile name when running commands:

```powershell
aws sts get-caller-identity --profile AdministratorAccess-251986419027
```

The AWS CLI automatically loads the cached SSO credentials. If you prefer, you can set `AWS_PROFILE=AdministratorAccess-251986419027` in your shell session to make this profile the default.

## 5. Refreshing sessions
SSO credentials expire periodically. When that happens, rerun `aws sso login --profile AdministratorAccess-251986419027` to refresh the cache.

## 6. Optional: Environment variable override
If you do set `AWS_PROFILE` or `AWS_DEFAULT_PROFILE`, it must match the profile name in your config file. Leave the key-based environment variables unset when using SSO to avoid conflicts.

## 7. Clean up cached credentials
Cached SSO tokens are stored under `%USERPROFILE%\.aws\sso\cache`. If needed, you can delete the files in this folder to force a fresh login.

Following these steps allows you to authenticate with AWS via SSO without storing static credentials, keeping your environment secure while enabling CLI access.
