# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly. **Do not open a public issue.**

Email: [swikarsharma@gmail.com](mailto:swikarsharma@gmail.com)

Please include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

We will acknowledge your report within 48 hours and aim to provide a fix within 7 days for critical issues.

## Supported Versions

| Version | Supported |
|---------|-----------|
| Latest  | ✅        |

## Security Considerations

This system handles legislative data. Key security areas:

- **Authentication** - Currently demo-mode (localStorage). Production will use SSO/OAuth.
- **Authorization** - Role-based access control enforced at route and API level.
- **Data Integrity** - All bill transitions are logged with audit trails.
- **Input Validation** - All user inputs are validated and sanitized.
