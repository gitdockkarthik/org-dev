-- Migration: Create incident_management schema and incidents table
-- Purpose: Persist incident tickets from genuine alerts for tracking and RCA
-- Section 3: Core incident tracking schema with recurrence and relation tracking

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS incident_management;

CREATE TABLE IF NOT EXISTS incident_management.incidents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id TEXT NOT NULL,
    alert_signature TEXT,
    status TEXT NOT NULL,
    priority TEXT,
    title TEXT,
    alert_payload JSONB,
    rca_findings JSONB,
    action_outcome JSONB,
    detected_at TIMESTAMPTZ,
    escalated_at TIMESTAMPTZ,
    rca_completed_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    recurrence_count INTEGER NOT NULL DEFAULT 1,
    related_ticket_id UUID REFERENCES incident_management.incidents(id),
    resolved_externally BOOLEAN NOT NULL DEFAULT FALSE,
    source_tool TEXT,
    environment TEXT,
    affected_resource TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_incidents_alert_id ON incident_management.incidents(alert_id);

ALTER TABLE incident_management.incidents ADD COLUMN IF NOT EXISTS resolved_externally BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE incident_management.incidents ADD COLUMN IF NOT EXISTS source_tool TEXT;
ALTER TABLE incident_management.incidents ADD COLUMN IF NOT EXISTS environment TEXT;
ALTER TABLE incident_management.incidents ADD COLUMN IF NOT EXISTS affected_resource TEXT;
