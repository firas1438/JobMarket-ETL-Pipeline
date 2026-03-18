-- Useful queries for dashboard and pipeline

-- Total jobs
-- SELECT COUNT(*) FROM jobs_clean;

-- Jobs over time
-- SELECT DATE(publication_date) AS day, COUNT(*) AS jobs
-- FROM jobs_clean
-- WHERE publication_date IS NOT NULL
-- GROUP BY 1 ORDER BY 1;

-- Top companies
-- SELECT company, COUNT(*) AS jobs
-- FROM jobs_clean
-- WHERE company IS NOT NULL AND company <> ''
-- GROUP BY 1 ORDER BY jobs DESC LIMIT 10;

-- Jobs by role
-- SELECT role_type, COUNT(*) AS jobs
-- FROM jobs_clean
-- GROUP BY 1 ORDER BY jobs DESC;

-- Top skills
-- SELECT skill, COUNT(*) AS jobs
-- FROM jobs_clean, UNNEST(skills_extracted) AS skill
-- GROUP BY 1 ORDER BY jobs DESC LIMIT 10;

