/*
populate file data_validation.sql with validation queries
Include basic checks for missing data, invalid values or referential
integrity violations. Be pedantic.
*/

-- Pedantic data validation queries for users, projects, tasks.
-- Run these queries to find rows violating expectations. Each block returns a label and offending rows/counts.

-- 1) Missing required values ---------------------------------------------------
-- Users: missing username/email
SELECT 'users: missing username or email' AS check, id, username, email, created_at
FROM users
WHERE username IS NULL OR btrim(username) = '' OR email IS NULL OR btrim(email) = '';

-- Projects: missing name or owner_id
SELECT 'projects: missing name or owner_id' AS check, id, name, owner_id, created_at
FROM projects
WHERE name IS NULL OR btrim(name) = '' OR owner_id IS NULL;

-- Tasks: missing title, project_id, status, or priority
SELECT 'tasks: missing required columns' AS check, id, project_id, title, status, priority, created_at
FROM tasks
WHERE title IS NULL OR btrim(title) = '' OR project_id IS NULL OR status IS NULL OR priority IS NULL;


-- 2) Duplicate unique constraints ------------------------------------------------
-- Duplicate usernames
SELECT 'users: duplicate usernames' AS check, username, array_agg(id) AS ids, count(*) AS cnt
FROM users
GROUP BY username
HAVING username IS NOT NULL AND count(*) > 1;

-- Duplicate emails
SELECT 'users: duplicate emails' AS check, email, array_agg(id) AS ids, count(*) AS cnt
FROM users
GROUP BY email
HAVING email IS NOT NULL AND count(*) > 1;


-- 3) Format and sanity checks ---------------------------------------------------
-- Basic email format (very permissive regex) - flags invalid emails
SELECT 'users: invalid email format' AS check, id, email
FROM users
WHERE email IS NOT NULL
  AND NOT (email ~ '^[^@\s]+@[^@\s]+\.[^@\s]+$');

-- Usernames with leading/trailing whitespace or zero-length after trim
SELECT 'users: username whitespace issues' AS check, id, username
FROM users
WHERE username IS NOT NULL AND (username <> btrim(username) OR btrim(username) = '');

-- Tasks: status values outside allowed set
SELECT 'tasks: invalid status values' AS check, id, status
FROM tasks
WHERE status IS NOT NULL AND status NOT IN ('todo','in_progress','done','blocked');

-- Tasks: priority outside 1..5
SELECT 'tasks: invalid priority range' AS check, id, priority
FROM tasks
WHERE priority IS NOT NULL AND (priority < 1 OR priority > 5);


-- 4) Referential integrity / orphan rows ---------------------------------------
-- Projects with non-existent owner
SELECT 'projects: owner_id refers to missing user' AS check, p.id AS project_id, p.owner_id
FROM projects p
LEFT JOIN users u ON p.owner_id = u.id
WHERE p.owner_id IS NOT NULL AND u.id IS NULL;

-- Tasks with non-existent project
SELECT 'tasks: project_id refers to missing project' AS check, t.id AS task_id, t.project_id
FROM tasks t
LEFT JOIN projects p ON t.project_id = p.id
WHERE t.project_id IS NOT NULL AND p.id IS NULL;

-- Tasks assigned to non-existent users (assignee_id not null)
SELECT 'tasks: assignee_id refers to missing user' AS check, t.id AS task_id, t.assignee_id
FROM tasks t
LEFT JOIN users u ON t.assignee_id = u.id
WHERE t.assignee_id IS NOT NULL AND u.id IS NULL;


-- 5) Temporal and logical inconsistencies --------------------------------------
-- Rows with created_at in the future (clock drift / bad inserts)
SELECT 'future timestamps: users' AS check, id, created_at
FROM users
WHERE created_at > now();

SELECT 'future timestamps: projects' AS check, id, created_at
FROM projects
WHERE created_at > now();

SELECT 'future timestamps: tasks' AS check, id, created_at
FROM tasks
WHERE created_at > now();

-- Tasks with due_date earlier than created_at (likely data error)
SELECT 'tasks: due_date before created_at' AS check, id, created_at, due_date
FROM tasks
WHERE due_date IS NOT NULL AND created_at IS NOT NULL AND due_date < created_at::date;


-- 6) Referential consistency and warnings --------------------------------------
-- Projects that currently have no tasks (might be OK, but good to review)
SELECT 'projects: zero tasks (warning)' AS check, p.id AS project_id, p.name, p.owner_id
FROM projects p
LEFT JOIN tasks t ON p.id = t.project_id
GROUP BY p.id, p.name, p.owner_id
HAVING count(t.id) = 0;

-- Tasks assigned to the same user as project owner (possible but report)
SELECT 'tasks: assignee is project owner (possible review)' AS check, t.id AS task_id, t.assignee_id, p.owner_id, p.id AS project_id
FROM tasks t
JOIN projects p ON t.project_id = p.id
WHERE t.assignee_id IS NOT NULL AND t.assignee_id = p.owner_id;


-- 7) Quick counts to gauge dataset health --------------------------------------
SELECT 'counts: users' AS what, count(*) AS cnt FROM users;
SELECT 'counts: projects' AS what, count(*) AS cnt FROM projects;
SELECT 'counts: tasks' AS what, count(*) AS cnt FROM tasks;

-- End of checks. Fix rows returned by the queries above to bring the dataset into compliance.
