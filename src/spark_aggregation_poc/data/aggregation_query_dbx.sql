(select sub_query.calculated_group_identifier as calculated_group_identifier,
        sub_query.calculated_finding_id       as calculated_finding_id,
        sub_query.finding_ids_without_group   as finding_ids_without_group,
        aggregation_groups.id                 as group_id,
        aggregation_groups.main_finding_id    as main_finding_id
 from (select md5(concat_ws('-',
                            '1',
                            findings.package_name))    as calculated_group_identifier,
              concat_ws('-',
                        '1',
                        findings.package_name,
                        'findings.package_name',
                        '',
                        '',
                        '2025/08/07T13/50/14.541213Z') as calculated_finding_id,
              coalesce(array_agg(findings.id) filter(
                  where aggregation_groups.id is null),
                       ARRAY[] ::integer[])            as finding_ids_without_group
       from postgres_prod_eu_002.seemplicitydemo.findings
                left outer join postgres_prod_eu_002.seemplicitydemo.finding_sla_rule_connections on
           findings.id = finding_sla_rule_connections.finding_id
                join (select plain_resources.id                          as id,
                             plain_resources.cloud_account               as root_cloud_account,
                             plain_resources.cloud_account_friendly_name as root_cloud_account_friendly_name
                      from postgres_prod_eu_002.seemplicitydemo.plain_resources
                      where true) as resources on
           findings.main_resource_id = resources.id
                join postgres_prod_eu_002.seemplicitydemo.findings_scores on
           findings.id = findings_scores.finding_id
                join postgres_prod_eu_002.seemplicitydemo.user_status on
           user_status.id = findings.id
                left outer join postgres_prod_eu_002.seemplicitydemo.findings_additional_data on
           findings.id = findings_additional_data.finding_id
                join postgres_prod_eu_002.seemplicitydemo.statuses on
           statuses.key = user_status.actual_status_key
                left outer join postgres_prod_eu_002.seemplicitydemo.aggregation_groups on
           findings.aggregation_group_id = aggregation_groups.id
                left outer join postgres_prod_eu_002.seemplicitydemo.findings_info on
           findings_info.id = findings.id
       where (findings.id not in (select anon_1.finding_id
                                  from (select aggregation_rules_findings_excluder.finding_id as finding_id
                                        from postgres_prod_eu_002.seemplicitydemo.aggregation_rules_findings_excluder
                                        where aggregation_rules_findings_excluder.aggregation_rule_id = 10) as anon_1))
--         and findings.id >= 1
--         and findings.id <= 100
         and findings.package_name is not null
       group by findings.package_name) as sub_query
          left outer join postgres_prod_eu_002.seemplicitydemo.aggregation_groups on
     aggregation_groups.group_identifier = sub_query.calculated_group_identifier
         and aggregation_groups.is_locked is false
         limit 10) as findings_data_alias