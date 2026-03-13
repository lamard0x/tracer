# Security Rules

## 26. No Hardcoded Secrets
API keys, passwords, tokens → env vars.

## 27. All User Inputs Validated
Zod, parameterized queries.

## 28. SQL Injection Prevention
Parameterized queries only.

## 29. XSS Prevention
Sanitized HTML output.

## 30. CSRF Protection
Enabled on all forms/endpoints.

## 31. Auth Verified
Authentication + authorization checked.

## 32. Rate Limiting
On all public endpoints.

## 33. Error Messages Safe
Không leak sensitive data trong errors.

## 34. Security Protocol
STOP → security-reviewer agent → fix CRITICAL → rotate secrets → review codebase.
