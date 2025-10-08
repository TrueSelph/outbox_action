# Outbox Action

![GitHub release (latest by date)](https://img.shields.io/github/v/release/TrueSelph/outbox_action)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/TrueSelph/outbox_action/test-action.yaml)
![GitHub issues](https://img.shields.io/github/issues/TrueSelph/outbox_action)
![GitHub pull requests](https://img.shields.io/github/issues-pr/TrueSelph/outbox_action)
![GitHub](https://img.shields.io/github/license/TrueSelph/outbox_action)

This action handles the queueing and scheduled mass dispatch of messages (WhatsApp, email, SMS, etc) to recipients. The package is a singleton and requires the Jivas library version ^2.1.0.

## Package Information

- **Name:** `jivas/outbox_action`
- **Author:** [V75 Inc.](https://v75inc.com/)
- **Architype:** `OutboxAction`
- **Version:** `0.1.0`

## Meta Information

- **Title:** Outbox Action
- **Description:** Handles the queueing and scheduled mass dispatch of messages (WhatsApp, email, SMS, etc) to recipients.
- **Group:** core
- **Type:** action

## Configuration

- **Singleton:** true

## Dependencies

- **Jivas:** `~2.1.14`
- **Actions:**
  - `jivas/pulse_action`: `~0.1.0`
