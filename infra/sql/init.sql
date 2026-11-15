-- ── AIM OS Database Schema ──────────────────────

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Projects
CREATE TABLE IF NOT EXISTS projects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    goal TEXT,
    status TEXT DEFAULT 'active',
    tags TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Memories
CREATE TABLE IF NOT EXISTS memories (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    content TEXT NOT NULL,
    tier TEXT DEFAULT 'short' CHECK (tier IN ('working','short','long','archive')),
    importance FLOAT DEFAULT 0.5,
    confidence FLOAT DEFAULT 0.5,
    source TEXT,
    project_id UUID REFERENCES projects(id),
    tags TEXT[],
    embedding_id TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    version INT DEFAULT 1
);

-- Timeline Events
CREATE TABLE IF NOT EXISTS timeline_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_type TEXT NOT NULL,
    description TEXT,
    app TEXT,
    project_id UUID REFERENCES projects(id),
    metadata JSONB DEFAULT '{}',
    started_at TIMESTAMPTZ DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    focus_score FLOAT DEFAULT 0.0
);

-- Tasks
CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID REFERENCES projects(id),
    title TEXT NOT NULL,
    status TEXT DEFAULT 'todo' CHECK (status IN ('todo','in_progress','done','cancelled')),
    priority TEXT DEFAULT 'medium' CHECK (priority IN ('low','medium','high','urgent')),
    deadline TIMESTAMPTZ,
    parent_id UUID REFERENCES tasks(id),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Timer Sessions
CREATE TABLE IF NOT EXISTS timer_sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    mode TEXT CHECK (mode IN ('pomodoro','focus','countdown','stopwatch')),
    project_id UUID REFERENCES projects(id),
    started_at TIMESTAMPTZ DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    duration_seconds INT,
    interruptions INT DEFAULT 0,
    focus_score FLOAT DEFAULT 0.0,
    notes TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_memories_tier ON memories(tier);
CREATE INDEX IF NOT EXISTS idx_memories_project ON memories(project_id);
CREATE INDEX IF NOT EXISTS idx_memories_importance ON memories(importance DESC);
CREATE INDEX IF NOT EXISTS idx_timeline_started ON timeline_events(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_timeline_project ON timeline_events(project_id);
CREATE INDEX IF NOT EXISTS idx_tasks_project ON tasks(project_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);

-- Default project
INSERT INTO projects (name, goal, status) 
VALUES ('AIM OS', 'Build the AIM operating environment', 'active')
ON CONFLICT DO NOTHING;