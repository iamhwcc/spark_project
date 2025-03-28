WITH RECURSIVE lineage_tree AS (
    -- 初始节点：从目标表开始回溯
    SELECT join_name,
           relation_join_name,
           1 AS lineage_level
    FROM table_lineage
    WHERE join_name = 'rpt.rpt_order_dashboard'

    UNION ALL

    -- 每次回溯一层上游
    SELECT a.join_name,
           a.relation_join_name,
           b.lineage_level + 1
    FROM table_lineage a
             JOIN lineage_tree b
                  ON a.join_name = b.relation_join_name)
SELECT DISTINCT relation_join_name AS upstream_table, lineage_level
FROM lineage_tree;
