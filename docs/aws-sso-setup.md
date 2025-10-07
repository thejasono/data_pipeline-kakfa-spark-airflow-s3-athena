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
Use the AWS CLI to establish a session for the configured profile:

```powershell
aws sso login --profile AdministratorAccess-251986419027
```

This command opens a browser window where you sign in to your Identity Center. After approval, the CLI caches temporary credentials locally.

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
