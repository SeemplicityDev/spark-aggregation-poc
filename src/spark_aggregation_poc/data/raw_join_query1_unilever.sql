SELECT
    findings.id as finding_id,
    findings.package_name as package_name,
    findings.main_resource_id,
    findings.aggregation_group_id,
    findings.finding_type_str as finding_type_str,
    findings.fix_subtype as fix_subtype,
    statuses.category as category,
    findings.fix_id as fix_id,
    findings.fix_type as fix_type
FROM findings
WHERE findings.package_name IS NOT NULL
