-- Counts must agree; do not hard-code the supplied snapshot counts.
SELECT
  (SELECT COUNT(*) FROM ai_dev_class_demo.bronze_layer.player_master) AS bronze_players,
  (SELECT COUNT(*) FROM ai_dev_class_demo.bronze_layer.regions) AS bronze_regions,
  (SELECT COUNT(*) FROM ai_dev_class_demo.silver_layer.player_silver) AS silver_players;

-- Expect zero rows.
SELECT player_id, COUNT(*) AS occurrences
FROM ai_dev_class_demo.silver_layer.player_silver
GROUP BY player_id HAVING COUNT(*) > 1;

-- Expect zero rows.
SELECT s.player_id, s.home_region_id
FROM ai_dev_class_demo.silver_layer.player_silver s
LEFT JOIN ai_dev_class_demo.bronze_layer.regions r ON s.home_region_id = r.id
WHERE s.home_region_id IS NOT NULL AND r.id IS NULL;

SELECT player_id, first_name, last_name, home_region_id,
       country_code, region_name, state_province_code
FROM ai_dev_class_demo.silver_layer.player_silver LIMIT 20;

-- Expect zero: nullable home regions retain players and have no enrichment.
SELECT COUNT(*) AS unexpected_enrichment
FROM ai_dev_class_demo.silver_layer.player_silver
WHERE home_region_id IS NULL AND (
  country_code IS NOT NULL OR region_type IS NOT NULL OR region_name IS NOT NULL
  OR state_province_code IS NOT NULL OR population IS NOT NULL
  OR latitude IS NOT NULL OR longitude IS NOT NULL);

-- Historical findings remain even after a later clean run.
SELECT * FROM ai_dev_class_demo.silver_layer.player_master_dq_issues
ORDER BY run_timestamp DESC LIMIT 20;
SELECT * FROM ai_dev_class_demo.silver_layer.regions_dq_issues
ORDER BY run_timestamp DESC LIMIT 20;

-- Compare all 26 columns, their order and types to specification section 3.
DESCRIBE TABLE ai_dev_class_demo.silver_layer.player_silver;
DESCRIBE DETAIL ai_dev_class_demo.silver_layer.player_silver;
