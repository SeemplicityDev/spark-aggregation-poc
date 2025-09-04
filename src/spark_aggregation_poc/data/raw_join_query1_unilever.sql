SELECT
    findings.id as finding_id,
    findings.package_name as package_name,
    findings.main_resource_id,
    findings.aggregation_group_id,
    findings.finding_type_str as finding_type_str,
    findings.fix_subtype as fix_subtype,
    findings.fix_id as fix_id,
    findings.fix_type as fix_type,

 datasource_id, datasource_definition_id, title, "source", finding_id, original_finding_id, region, created_time, discovered_time, due_date, last_reported_time, original_status, time_to_remediate, category, sub_category, cloud_provider, cloud_account, cloud_account_friendly_name, dictionary_category, dictionary_sub_category, rule_id, resource_reported_not_exist,   image_id, scan_id, editable, reopen_date,   fix_vendor_id,  rule_type, rule_family, last_collected_time
FROM unilever.findings;

FROM findings
WHERE findings.package_name IS NOT NULL
