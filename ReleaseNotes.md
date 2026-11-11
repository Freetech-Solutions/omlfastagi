# Release Notes
2026-11-09

## Removed

* **Dialplan alignment cleanup:** Removed FastAGI handlers no longer invoked from ACD dialplan (`oml_extensions.conf`):
  `variables`, `channel-variables`, `omni-retrieve-value`, `omni-crm-customer-vars`,
  `omni-conference-ids`, `omni-dial2multinum`, `omni-notify-multinum-answer`, `omni-amd`.
* **Dead helpers:** Removed calldata Redis helpers and unused date/HTTP/Gearman code paths.
* **Dependencies:** Dropped `gearman3`, `pytz`, `requests`, and `urllib3` from FastAGI requirements.
* **Runtime env:** FastAGI no longer needs `GEARMAN_JOB_SERVERS`, `DIALER_AMD_ENDPOINT`, or `OMNILEADS_HOSTNAME`.

## Kept (dialplan-active)

* `omni-retrieve-conf` — Redis family → Asterisk channel variables (`sub-oml-set-chanvars`).
* `omni-survey-answer` — persist survey answers to PostgreSQL (`sub-oml-module-survey`).

## Changed

## Fixed

---

2025-03-13

## Added

* **oml-2885 Multinumber Call Detection and Channel Variable Setting:** Introduced a new function to detect successful calls in multinumber dialing scenarios and set corresponding channel variables. This allows for more granular call handling and tracking on dialplan.
* **oml-2885 Agent Contact View Enhancement:** Implemented a new function to display the specific number that answered a multinumber call within the agent's contact view. This provides agents with immediate and relevant information about the call.

## Changed

## Fixed

## Removed
