SELECT
    findings.id as finding_id,
    findings.package_name,
    findings.main_resource_id,
    findings.aggregation_group_id,
    findings.source as source,
    findings.rule_id as rule_id,
    findings.category as category,
    findings.rule_family as rule_family
FROM findings
WHERE findings.package_name IS NOT NULL