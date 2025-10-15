SELECT sub_query.calculated_group_identifier as calculated_group_identifier,
                              sub_query.calculated_finding_id       as calculated_finding_id,
                              sub_query.finding_ids_without_group   as finding_ids_without_group,
                              aggregation_groups.id                 as group_id,
                              aggregation_groups.main_finding_id    as main_finding_id
                       FROM (SELECT md5(concat_ws('-',
                                                  '1',
                                                  findings.package_name))        as calculated_group_identifier,
                                 concat_ws('-',
                                              '1',
                                              findings.package_name,
                                              'findings.package_name', '','','2025/08/07T13/50/14.541213Z')     as calculated_finding_id,
                                 coalesce(collect_list(findings.id), array()) as finding_ids_without_group
                             FROM partitioned_findings as findings
                                      LEFT OUTER JOIN finding_sla_rule_connections ON
                                          findings.id = finding_sla_rule_connections.finding_id
                                 JOIN (SELECT plain_resources.id                          as id,
                                              plain_resources.cloud_account               as root_cloud_account,
                                              plain_resources.cloud_account_friendly_name as root_cloud_account_friendly_name
                                       FROM plain_resources
                                       WHERE true) as resources ON
                                           findings.main_resource_id = resources.id
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
                             WHERE (findings.id NOT IN (SELECT aggregation_rules_findings_excluder.finding_id
                                                        FROM aggregation_rules_findings_excluder
                                                        WHERE aggregation_rules_findings_excluder.aggregation_rule_id = 10))
                               AND findings.package_name IS NOT NULL
                             GROUP BY findings.package_name) as sub_query
                                LEFT OUTER JOIN aggregation_groups ON
                           aggregation_groups.group_identifier = sub_query.calculated_group_identifier
                               AND aggregation_groups.is_locked IS FALSE