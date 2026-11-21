---
title: Data Platform Metrics FAQ
doc_id: metrics-faq
partner_scope: [ALL]
classification: internal
owner: Data Engineering
updated: 2026-11-01
---

# Data platform metrics FAQ

## Gross booking value
Gross booking value (GBV) is the sum of booking amounts in USD for bookings that are not cancelled. Amounts in other currencies are converted with the reference FX table at load time. Cancelled bookings are excluded from GBV and from the bookings count but are included in the cancellation rate.

## Refresh schedule
The lakehouse refreshes nightly at 02:00 UTC. Consent events are processed continuously, so contactability can change between nightly refreshes. Use the data_freshness tool to see the as-of date and open data quality issues.

## Why numbers can differ from a partner's own report
Partners report in their own currency and may count cancelled bookings. Our metrics are USD-normalised and exclude cancellations. Members held in quarantine for data quality reasons (for example an unknown country) are not counted until the source system is fixed.
