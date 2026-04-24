# Security Policy

## Supported Versions

Security fixes are applied to the latest `main` branch.

## Reporting A Vulnerability

Do not open public GitHub issues for suspected vulnerabilities.

Please report security issues privately by email:
- Anthony Mockler: `amockler@unicef.org`
- Hugo Ruiz Verastegui: `huruiz@unicef.org`

Include the following details:
1. Affected file(s) and branch/commit.
2. Reproduction steps.
3. Expected versus observed behavior.
4. Impact assessment (confidentiality, integrity, availability).
5. Any suggested remediation.

## Disclosure Process

1. Acknowledge report receipt within 3 business days.
2. Validate and triage severity.
3. Prepare patch and regression checks.
4. Coordinate disclosure timing with reporter.
5. Publish fix with summary in release notes.

## Security Scope Notes

This repository processes external data feeds and uses API credentials supplied by operators at runtime. Secrets must never be committed to source control.
