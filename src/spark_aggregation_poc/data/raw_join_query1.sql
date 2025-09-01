SELECT
    findings.id as finding_id,
    findings.package_name as package_name,
    findings.main_resource_id,
    findings.aggregation_group_id,
    finding_sla_rule_connections.finding_id as sla_connection_id,
    plain_resources.id as resource_id,
    plain_resources.cloud_account as root_cloud_account,
    plain_resources.cloud_account_friendly_name as root_cloud_account_friendly_name,
    findings_scores.finding_id as score_finding_id,
    user_status.id as user_status_id,
    user_status.actual_status_key,
    findings_additional_data.finding_id as additional_data_id,
    statuses.key as status_key,
    aggregation_groups.id as existing_group_id,
    aggregation_groups.main_finding_id as existing_main_finding_id,
    aggregation_groups.group_identifier as existing_group_identifier,
    aggregation_groups.is_locked,
    findings_info.id as findings_info_id,
    findings.finding_type_str as finding_type_str,
    findings.fix_subtype as fix_subtype,
    statuses.category as category,
    findings.fix_id as fix_id,
    findings_additional_Data.cve[1] as cve,
    findings.fix_type as fix_type

FROM findings
LEFT OUTER JOIN finding_sla_rule_connections ON
    findings.id = finding_sla_rule_connections.finding_id
JOIN plain_resources ON
    findings.main_resource_id = plain_resources.id
JOIN findings_scores ON
    findings.id = findings_scores.finding_id
JOIN user_status ON
    user_status.id = findings.id
LEFT OUTER JOIN findings_additional_data ON
    findings.id = findings_additional_data.finding_id
JOIN statuses ON
    statuses.key = user_status.actual_status_key
LEFT OUTER JOIN aggregation_groups ON
    findings.aggregation_group_id = aggregation_groups.id
LEFT OUTER JOIN findings_info ON
    findings_info.id = findings.id
LEFT OUTER JOIN aggregation_rules_findings_excluder ON
    findings.id = aggregation_rules_findings_excluder.finding_id
WHERE findings.package_name IS NOT NULL