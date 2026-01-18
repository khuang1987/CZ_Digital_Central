CREATE TABLE IF NOT EXISTS planner_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    TaskId TEXT UNIQUE,
    TaskName TEXT,
    BucketName TEXT,
    Status TEXT,
    Priority TEXT,
    Assignees TEXT,
    CreatedBy TEXT,
    CreatedDate DATE,
    StartDate DATE,
    DueDate DATE,
    IsRecurring BOOLEAN,
    IsLate BOOLEAN,
    CompletedDate DATE,
    CompletedBy TEXT,
    CompletedChecklistItemCount INTEGER,
    ChecklistItemCount INTEGER,
    Labels TEXT,
    Description TEXT,
    SourceFile TEXT,
    TeamName TEXT,
    ImportedAt DATETIME,
    IsDeleted BOOLEAN DEFAULT 0,
    DeletedAt DATETIME,
    LastSeenAt DATETIME,
    LastSeenSourceMtime DATETIME,
    LastSeenSourceFile TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS planner_task_labels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    TaskId TEXT,
    OriginalLabel TEXT,
    CleanedLabel TEXT,
    IsExcluded BOOLEAN DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (TaskId) REFERENCES planner_tasks(TaskId) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_planner_labels_taskid ON planner_task_labels(TaskId);
CREATE INDEX IF NOT EXISTS idx_planner_labels_cleaned ON planner_task_labels(CleanedLabel);
