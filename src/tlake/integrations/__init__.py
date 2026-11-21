"""Outbound connectors for marketing technology (Adobe Marketo Engage, Tealium).

Each connector has a live client (HTTP, credentials from environment variables) and a
dry-run client with the same interface that records the requests it would have sent.
Nothing here decides WHO may be contacted: that is audience.export's job (role check and
consent re-check). Connectors only deliver an already-approved list.
"""
