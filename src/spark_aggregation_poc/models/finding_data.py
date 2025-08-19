from dataclasses import dataclass


@dataclass
class FindingData:
    calculated_group_identifier: str
    calculated_finding_id: str
    finding_ids_without_group: [int]
    group_id: int
    main_finding_id: int