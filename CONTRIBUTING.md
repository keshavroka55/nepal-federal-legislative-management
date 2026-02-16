# Contributing to Nepal Legislature

Thank you for your interest in contributing to the Nepal Federal Legislative Management System! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Making Changes](#making-changes)
- [Commit Guidelines](#commit-guidelines)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Reporting Bugs](#reporting-bugs)
- [Requesting Features](#requesting-features)

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md). Please read it before contributing.

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone git@github.com:YOUR_USERNAME/nepal-legislature.git
   cd nepal-legislature
   ```
3. Add the upstream remote:
   ```bash
   git remote add upstream git@github.com:swikars1/nepal-legislature.git
   ```
4. Create a new branch for your work:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Setup

### Prerequisites

- Node.js 18+ or [Bun](https://bun.sh)
- PostgreSQL 15+ (optional - mock data layer works without it)
- Git

### Installation

```bash
# Install dependencies
bun install

# Copy environment file
cp .env.example .env

# Run development server
bun dev
```

The app will be available at [http://localhost:3000](http://localhost:3000).

### Database Setup (Optional)

If you want to work with real database features:

```bash
# Generate Prisma client
bunx prisma generate

# Run database migrations
bunx prisma db push

# Seed the database
bunx prisma db seed
```

## Project Structure

```
src/
├── app/              # Next.js App Router pages
├── components/       # Reusable UI components
│   ├── layout/       # Layout components (Sidebar, Topbar)
│   └── providers/    # Context providers (Auth)
├── lib/              # Core business logic
│   ├── auth.ts       # Authentication utilities
│   ├── mock-data.ts  # Mock data for development
│   ├── prisma.ts     # Database client
│   ├── state-machine/# Bill lifecycle state machine
│   └── rules-engine/ # Constitutional deadline rules
├── types/            # TypeScript type definitions
└── prisma/
    └── schema.prisma # Database schema
```

## Making Changes

1. **Stay up to date** with the upstream repository:
   ```bash
   git fetch upstream
   git rebase upstream/master
   ```

2. **Make focused changes** - each PR should address a single concern

3. **Test your changes** - ensure the build passes:
   ```bash
   bun run build
   ```

4. **Check for lint errors**:
   ```bash
   bun run lint
   ```

## Commit Guidelines

We follow [Conventional Commits](https://www.conventionalcommits.org/). Each commit message should be structured as:

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Types

| Type | Description |
|------|-------------|
| `feat` | A new feature |
| `fix` | A bug fix |
| `docs` | Documentation changes |
| `style` | Code style changes (formatting, no logic change) |
| `refactor` | Code refactoring |
| `perf` | Performance improvements |
| `test` | Adding or updating tests |
| `chore` | Build process, tooling, or dependency changes |

### Examples

```
feat(bills): add amendment proposal workflow
fix(auth): handle expired session redirect
docs(readme): update installation instructions
refactor(state-machine): simplify transition guards
```

## Pull Request Process

1. **Update documentation** if your changes affect the public API or user-facing features
2. **Fill out the PR template** completely
3. **Ensure CI passes** - the build must compile without errors
4. **Request review** from at least one maintainer
5. **Address feedback** promptly and push updates to the same branch

### PR Title Format

Use the same convention as commits:
```
feat(scope): short description of the change
```

### PR Checklist

- [ ] Code compiles without errors (`bun run build`)
- [ ] Lint passes (`bun run lint`)
- [ ] Changes are documented where appropriate
- [ ] Commit messages follow conventional commits
- [ ] PR description explains the "why" behind changes

## Coding Standards

### TypeScript

- Use strict TypeScript - avoid `any` types
- Define interfaces for all data structures in `src/types/`
- Use named exports over default exports
- Prefer `const` over `let`, never use `var`

### React / Next.js

- Use functional components with hooks
- Use CSS Modules for component-specific styles
- Use the global design system variables from `globals.css`
- Mark client components with `'use client'` only when needed
- Keep components focused and under 200 lines

### CSS

- Use CSS custom properties defined in `globals.css`
- Follow the existing naming conventions (BEM-inspired)
- Use CSS Modules for component-level styles
- Avoid inline styles

### File Naming

- Components: `PascalCase.tsx` (e.g., `Sidebar.tsx`)
- Styles: `component-name.module.css` (e.g., `sidebar.module.css`)
- Utilities: `camelCase.ts` (e.g., `mockData.ts`)
- Pages: `page.tsx` (Next.js convention)

## Reporting Bugs

Use the [Bug Report](.github/ISSUE_TEMPLATE/bug_report.md) template when filing issues. Include:

- Steps to reproduce
- Expected vs actual behavior
- Screenshots if applicable
- Browser and OS information

## Requesting Features

Use the [Feature Request](.github/ISSUE_TEMPLATE/feature_request.md) template. Include:

- Problem description
- Proposed solution
- Alternatives considered
- Relevance to Nepal's legislative process

## Questions?

If you have questions about contributing, feel free to open a [Discussion](https://github.com/swikars1/nepal-legislature/discussions) on GitHub.

---

Thank you for helping improve Nepal's legislative management system! 🇳🇵
