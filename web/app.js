const state = {
  records: [],
  events: [],
  graph: null,
};

let cy = null;

const recordList = document.getElementById("record-list");
const recordTemplate = document.getElementById("record-template");
const descriptorTemplate = document.getElementById("descriptor-template");
const addRecordButton = document.getElementById("add-record");
const refreshButton = document.getElementById("refresh-graph");
const copyDebugButton = document.getElementById("copy-debug");

const identityKeysInput = document.getElementById("identity-keys");
const conflictAttrsInput = document.getElementById("conflict-attrs");
const viewStartInput = document.getElementById("view-start");
const viewEndInput = document.getElementById("view-end");
const ontologyPresetSelect = document.getElementById("ontology-preset");
const applyPresetButton = document.getElementById("apply-preset");

const statRecords = document.getElementById("stat-records");
const statClusters = document.getElementById("stat-clusters");
const statConflicts = document.getElementById("stat-conflicts");

const debugSummary = document.getElementById("debug-summary");
const debugRecords = document.getElementById("debug-records");
const debugGraph = document.getElementById("debug-graph");
const debugEvents = document.getElementById("debug-events");
const debugPresets = document.getElementById("debug-presets");

const presetSelect = document.getElementById("preset-select");
const loadPresetButton = document.getElementById("load-preset");
const appendPresetButton = document.getElementById("append-preset");
const loadJsonButton = document.getElementById("load-json");
const clearRecordsButton = document.getElementById("clear-records");
const presetJsonInput = document.getElementById("preset-json");
const graphJsonInput = document.getElementById("graph-json");
const loadGraphJsonButton = document.getElementById("load-graph-json");
const toast = document.getElementById("toast");

const graphContainer = document.getElementById("graph");
const edgeDetails = document.getElementById("edge-details");
const edgeType = document.getElementById("edge-type");
const edgeAttr = document.getElementById("edge-attr");
const edgeValues = document.getElementById("edge-values");
const edgeInterval = document.getElementById("edge-interval");
const edgeRecords = document.getElementById("edge-records");
const graphDescription = document.getElementById("graph-description");
const goldenSummary = document.getElementById("golden-summary");

const tabs = Array.from(document.querySelectorAll(".tab"));

const ontologyPresets = {
  iam: {
    identityKeys: ["email"],
    conflictAttrs: ["email", "phone", "ssn", "role"],
  },
};

const recordPresets = [
  {
    "id": "test_direct_conflict_creation",
    "label": "Direct conflict creation (no records)",
    "identityKeys": [],
    "conflictAttrs": [],
    "description": "No records are loaded; the graph should be empty and the stats should remain at zero.",
    "records": [],
    "graph": {
      "conflicts_with_edges": [],
      "metadata": {
        "num_clusters": "0",
        "num_records": "0",
        "timestamp": "2026-01-01T19:32:04.682431753+00:00",
        "version": "1.0"
      },
      "nodes": [],
      "same_as_edges": []
    }
  },
  {
    "id": "test_indirect_conflict_creation",
    "label": "Indirect conflict creation (no records)",
    "identityKeys": [],
    "conflictAttrs": [],
    "description": "No records are loaded; use this to confirm the graph clears cleanly between runs.",
    "records": [],
    "graph": {
      "conflicts_with_edges": [],
      "metadata": {
        "num_clusters": "0",
        "num_records": "0",
        "timestamp": "2026-01-01T19:32:04.682455927+00:00",
        "version": "1.0"
      },
      "nodes": [],
      "same_as_edges": []
    }
  },
  {
    "id": "test_observation_creation",
    "label": "Observation creation (no records)",
    "identityKeys": [],
    "conflictAttrs": [],
    "description": "Empty dataset; nothing should render besides the layout and legend.",
    "records": [],
    "graph": {
      "conflicts_with_edges": [],
      "metadata": {
        "num_clusters": "0",
        "num_records": "0",
        "timestamp": "2026-01-01T19:32:04.682462032+00:00",
        "version": "1.0"
      },
      "nodes": [],
      "same_as_edges": []
    }
  },
  {
    "id": "test_conflict_detection",
    "label": "Conflict detection (empty store)",
    "identityKeys": [],
    "conflictAttrs": [],
    "description": "Empty dataset; verify that no same-as or conflict edges appear.",
    "records": [],
    "graph": {
      "conflicts_with_edges": [],
      "metadata": {
        "num_clusters": "0",
        "num_records": "0",
        "timestamp": "2026-01-01T19:32:04.682466238+00:00",
        "version": "1.0"
      },
      "nodes": [],
      "same_as_edges": []
    }
  },
  {
    "id": "test_can_handle_indirect_conflict",
    "label": "Indirect conflict handling (name/country + phone/employee_id)",
    "identityKeys": [
      "name",
      "country"
    ],
    "conflictAttrs": [
      "phone",
      "employee_id"
    ],
    "description": "Three records share name+country; two CRM records disagree on phone over the same interval. Expect a same-as link across all three and a red phone conflict between the CRM records.",
    "records": [
      {
        "id": "R1",
        "uid": "1",
        "entityType": "person",
        "perspective": "crm_system",
        "descriptors": [
          {
            "attr": "name",
            "value": "John Doe",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "country",
            "value": "US",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "phone",
            "value": "555-1111",
            "start": 86400,
            "end": 432000
          }
        ]
      },
      {
        "id": "R2",
        "uid": "2",
        "entityType": "person",
        "perspective": "crm_system",
        "descriptors": [
          {
            "attr": "name",
            "value": "John Doe",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "country",
            "value": "US",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "phone",
            "value": "555-2222",
            "start": 86400,
            "end": 432000
          }
        ]
      },
      {
        "id": "R3",
        "uid": "3",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "name",
            "value": "John Doe",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "country",
            "value": "US",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "employee_id",
            "value": "EMP001",
            "start": 86400,
            "end": 432000
          }
        ]
      }
    ],
    "graph": {
      "conflicts_with_edges": [],
      "metadata": {
        "num_clusters": "1",
        "num_records": "3",
        "timestamp": "2026-01-01T19:32:04.682559889+00:00",
        "version": "1.0"
      },
      "nodes": [
        {
          "cluster_id": null,
          "id": "record_2",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "crm_system",
            "uid": "2"
          },
          "record_id": 2
        },
        {
          "cluster_id": null,
          "id": "record_1",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "crm_system",
            "uid": "1"
          },
          "record_id": 1
        },
        {
          "cluster_id": null,
          "id": "record_3",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "3"
          },
          "record_id": 3
        },
        {
          "cluster_id": 0,
          "id": "cluster_0",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "JD-V4",
            "cluster_key_identity": "preset",
            "golden": "[{\"attr\":\"country\",\"value\":\"US\",\"interval\":{\"start\":86400,\"end\":432000}},{\"attr\":\"employee_id\",\"value\":\"EMP001\",\"interval\":{\"start\":86400,\"end\":432000}},{\"attr\":\"name\",\"value\":\"John Doe\",\"interval\":{\"start\":86400,\"end\":432000}}]",
            "root_record": "R1",
            "size": "3"
          },
          "record_id": null
        }
      ],
      "same_as_edges": [
        {
          "interval": {
            "end": 9223372036854775807,
            "start": -9223372036854775808
          },
          "metadata": {},
          "reason": "cluster_member",
          "source": 3,
          "target": 2
        },
        {
          "interval": {
            "end": 9223372036854775807,
            "start": -9223372036854775808
          },
          "metadata": {},
          "reason": "cluster_member",
          "source": 3,
          "target": 1
        },
        {
          "interval": {
            "end": 9223372036854775807,
            "start": -9223372036854775808
          },
          "metadata": {},
          "reason": "cluster_member",
          "source": 2,
          "target": 1
        }
      ]
    }
  },
  {
    "id": "test_can_resolve_person_identity_conflicts",
    "label": "Person identity conflicts (EMP001 vs EMP002)",
    "identityKeys": [
      "name",
      "country"
    ],
    "conflictAttrs": [
      "employee_id",
      "email"
    ],
    "description": "Two HR records share name+country but overlap with different employee_id and email values. Expect a same-as link and red conflict edges during the overlapping years.",
    "records": [
      {
        "id": "R1",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "employee_id",
            "value": "EMP001",
            "start": 1388966400,
            "end": 1613520000
          },
          {
            "attr": "country",
            "value": "RU",
            "start": 1388966400,
            "end": 1613520000
          },
          {
            "attr": "currency",
            "value": "RUB",
            "start": 1388966400,
            "end": 1613520000
          },
          {
            "attr": "name",
            "value": "John Smith",
            "start": 1388966400,
            "end": 1613520000
          },
          {
            "attr": "department",
            "value": "Engineering",
            "start": 1388966400,
            "end": 1613520000
          },
          {
            "attr": "email",
            "value": "john.smith@company.com",
            "start": 1388966400,
            "end": 1613520000
          }
        ]
      },
      {
        "id": "R2",
        "uid": "2",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "employee_id",
            "value": "EMP002",
            "start": 1047340800,
            "end": 1438560000
          },
          {
            "attr": "country",
            "value": "RU",
            "start": 1047340800,
            "end": 1438560000
          },
          {
            "attr": "currency",
            "value": "RUB",
            "start": 1047340800,
            "end": 1320105600
          },
          {
            "attr": "currency",
            "value": "USD",
            "start": 1320105600,
            "end": 1438560000
          },
          {
            "attr": "name",
            "value": "John Smith",
            "start": 1047340800,
            "end": 1438560000
          },
          {
            "attr": "department",
            "value": "Engineering",
            "start": 1047628800,
            "end": 1438560000
          },
          {
            "attr": "email",
            "value": "j.smith@corp.com",
            "start": 1047340800,
            "end": 1438560000
          }
        ]
      }
    ],
    "graph": {
      "conflicts_with_edges": [],
      "metadata": {
        "num_clusters": "1",
        "num_records": "2",
        "timestamp": "2026-01-01T19:32:04.682694606+00:00",
        "version": "1.0"
      },
      "nodes": [
        {
          "cluster_id": null,
          "id": "record_1",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 1
        },
        {
          "cluster_id": null,
          "id": "record_2",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "2"
          },
          "record_id": 2
        },
        {
          "cluster_id": 0,
          "id": "cluster_0",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "JS-YS",
            "cluster_key_identity": "preset",
            "golden": "[{\"attr\":\"country\",\"value\":\"RU\",\"interval\":{\"start\":1047340800,\"end\":1613520000}},{\"attr\":\"currency\",\"value\":\"RUB\",\"interval\":{\"start\":1047340800,\"end\":1320105600}},{\"attr\":\"currency\",\"value\":\"RUB\",\"interval\":{\"start\":1438560000,\"end\":1613520000}},{\"attr\":\"currency\",\"value\":\"USD\",\"interval\":{\"start\":1320105600,\"end\":1388966400}},{\"attr\":\"department\",\"value\":\"Engineering\",\"interval\":{\"start\":1047628800,\"end\":1613520000}},{\"attr\":\"email\",\"value\":\"j.smith@corp.com\",\"interval\":{\"start\":1047340800,\"end\":1388966400}},{\"attr\":\"email\",\"value\":\"john.smith@company.com\",\"interval\":{\"start\":1438560000,\"end\":1613520000}},{\"attr\":\"employee_id\",\"value\":\"EMP001\",\"interval\":{\"start\":1438560000,\"end\":1613520000}},{\"attr\":\"employee_id\",\"value\":\"EMP002\",\"interval\":{\"start\":1047340800,\"end\":1388966400}},{\"attr\":\"name\",\"value\":\"John Smith\",\"interval\":{\"start\":1047340800,\"end\":1613520000}}]",
            "root_record": "R1",
            "size": "2"
          },
          "record_id": null
        }
      ],
      "same_as_edges": [
        {
          "interval": {
            "end": 9223372036854775807,
            "start": -9223372036854775808
          },
          "metadata": {},
          "reason": "cluster_member",
          "source": 2,
          "target": 1
        }
      ]
    }
  },
  {
    "id": "test_can_resolve_temporal_identity_conflicts",
    "label": "Temporal identity conflicts (name/email changes)",
    "identityKeys": [
      "name",
      "country"
    ],
    "conflictAttrs": [
      "employee_id",
      "phone",
      "email"
    ],
    "description": "Three records span different eras with matching name+country. Expect them to cluster via time-sliced same-as links but show no red conflicts because values do not overlap.",
    "records": [
      {
        "id": "R1",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "employee_id",
            "value": "H6TJG4721",
            "start": 851990400,
            "end": 1613520000
          },
          {
            "attr": "country",
            "value": "CH",
            "start": 851990400,
            "end": 1613520000
          },
          {
            "attr": "currency",
            "value": "CHF",
            "start": 851990400,
            "end": 1613520000
          },
          {
            "attr": "name",
            "value": "CH0002092614",
            "start": 851990400,
            "end": 1211760000
          },
          {
            "attr": "name",
            "value": "CH0039821084",
            "start": 1211760000,
            "end": 1613520000
          },
          {
            "attr": "department",
            "value": "XSWX",
            "start": 1363564800,
            "end": 1613520000
          },
          {
            "attr": "email",
            "value": "4582526",
            "start": 851990400,
            "end": 1211760000
          },
          {
            "attr": "email",
            "value": "B39HW28",
            "start": 1211760000,
            "end": 1613520000
          }
        ]
      },
      {
        "id": "R2",
        "uid": "2",
        "entityType": "person",
        "perspective": "crm_system",
        "descriptors": [
          {
            "attr": "phone",
            "value": "17518",
            "start": 978307200,
            "end": 1007078400
          },
          {
            "attr": "country",
            "value": "CH",
            "start": 978307200,
            "end": 1007078400
          },
          {
            "attr": "currency",
            "value": "CHF",
            "start": 978307200,
            "end": 1007078400
          },
          {
            "attr": "name",
            "value": "CH0002092614",
            "start": 978307200,
            "end": 1007078400
          }
        ]
      },
      {
        "id": "R3",
        "uid": "3",
        "entityType": "person",
        "perspective": "crm_system",
        "descriptors": [
          {
            "attr": "phone",
            "value": "4688",
            "start": 795484800,
            "end": 944006400
          },
          {
            "attr": "country",
            "value": "CH",
            "start": 795484800,
            "end": 944006400
          },
          {
            "attr": "currency",
            "value": "CHF",
            "start": 795484800,
            "end": 944006400
          },
          {
            "attr": "name",
            "value": "CH0002092614",
            "start": 795484800,
            "end": 944006400
          }
        ]
      }
    ],
    "graph": {
      "conflicts_with_edges": [],
      "metadata": {
        "num_clusters": "1",
        "num_records": "3",
        "timestamp": "2026-01-01T19:32:04.682823692+00:00",
        "version": "1.0"
      },
      "nodes": [
        {
          "cluster_id": null,
          "id": "record_1",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 1
        },
        {
          "cluster_id": null,
          "id": "record_2",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "crm_system",
            "uid": "2"
          },
          "record_id": 2
        },
        {
          "cluster_id": null,
          "id": "record_3",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "crm_system",
            "uid": "3"
          },
          "record_id": 3
        },
        {
          "cluster_id": 0,
          "id": "cluster_0",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "CH00-HA",
            "cluster_key_identity": "preset",
            "golden": "[{\"attr\":\"country\",\"value\":\"CH\",\"interval\":{\"start\":795484800,\"end\":1613520000}},{\"attr\":\"currency\",\"value\":\"CHF\",\"interval\":{\"start\":795484800,\"end\":1613520000}},{\"attr\":\"department\",\"value\":\"XSWX\",\"interval\":{\"start\":1363564800,\"end\":1613520000}},{\"attr\":\"email\",\"value\":\"4582526\",\"interval\":{\"start\":851990400,\"end\":1211760000}},{\"attr\":\"email\",\"value\":\"B39HW28\",\"interval\":{\"start\":1211760000,\"end\":1613520000}},{\"attr\":\"employee_id\",\"value\":\"H6TJG4721\",\"interval\":{\"start\":851990400,\"end\":1613520000}},{\"attr\":\"name\",\"value\":\"CH0002092614\",\"interval\":{\"start\":795484800,\"end\":1211760000}},{\"attr\":\"name\",\"value\":\"CH0039821084\",\"interval\":{\"start\":1211760000,\"end\":1613520000}},{\"attr\":\"phone\",\"value\":\"17518\",\"interval\":{\"start\":978307200,\"end\":1007078400}},{\"attr\":\"phone\",\"value\":\"4688\",\"interval\":{\"start\":795484800,\"end\":944006400}}]",
            "root_record": "R1",
            "size": "3"
          },
          "record_id": null
        }
      ],
      "same_as_edges": [
        {
          "interval": {
            "end": 9223372036854775807,
            "start": -9223372036854775808
          },
          "metadata": {},
          "reason": "cluster_member",
          "source": 1,
          "target": 3
        },
        {
          "interval": {
            "end": 9223372036854775807,
            "start": -9223372036854775808
          },
          "metadata": {},
          "reason": "cluster_member",
          "source": 1,
          "target": 2
        },
        {
          "interval": {
            "end": 9223372036854775807,
            "start": -9223372036854775808
          },
          "metadata": {},
          "reason": "cluster_member",
          "source": 3,
          "target": 2
        }
      ]
    }
  },
  {
    "id": "test_can_resolve_name_change_conflicts",
    "label": "Name change conflicts (AVESCO timeline)",
    "identityKeys": [
      "name",
      "country"
    ],
    "conflictAttrs": [
      "employee_id",
      "phone",
      "email"
    ],
    "description": "One long-lived record with multiple name changes aligns with a shorter CRM record. Expect a same-as link without red conflicts, highlighting the name timeline.",
    "records": [
      {
        "id": "R1",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "employee_id",
            "value": "X7W5YMBX4",
            "start": 851990400,
            "end": 1482451200
          },
          {
            "attr": "country",
            "value": "GB",
            "start": 851990400,
            "end": 1482451200
          },
          {
            "attr": "currency",
            "value": "GBP",
            "start": 851990400,
            "end": 1482451200
          },
          {
            "attr": "name",
            "value": "GB0000653229",
            "start": 851990400,
            "end": 1482451200
          },
          {
            "attr": "department",
            "value": "XLON",
            "start": 1363564800,
            "end": 1482451200
          },
          {
            "attr": "email",
            "value": "john.doe@company.com",
            "start": 851990400,
            "end": 1482451200
          },
          {
            "attr": "type",
            "value": "COMMON STOCK-S",
            "start": 851990400,
            "end": 1482451200
          },
          {
            "attr": "name",
            "value": "INVESTINMEDIA",
            "start": 851990400,
            "end": 1179705600
          },
          {
            "attr": "name",
            "value": "AVESCO GROUP",
            "start": 1179705600,
            "end": 1241136000
          },
          {
            "attr": "name",
            "value": "AVESCO GROUP PLC",
            "start": 1241136000,
            "end": 1482451200
          }
        ]
      },
      {
        "id": "R2",
        "uid": "2",
        "entityType": "person",
        "perspective": "crm_system",
        "descriptors": [
          {
            "attr": "phone",
            "value": "64541",
            "start": 946684800,
            "end": 1022889600
          },
          {
            "attr": "country",
            "value": "GB",
            "start": 946684800,
            "end": 1022889600
          },
          {
            "attr": "currency",
            "value": "GBP",
            "start": 946684800,
            "end": 1022889600
          },
          {
            "attr": "name",
            "value": "GB0000653229",
            "start": 946684800,
            "end": 1022889600
          },
          {
            "attr": "department",
            "value": "XLON",
            "start": 1019520000,
            "end": 1022889600
          },
          {
            "attr": "type",
            "value": "COMMON",
            "start": 980985600,
            "end": 1022889600
          },
          {
            "attr": "name",
            "value": "AVESCO",
            "start": 946684800,
            "end": 1022889600
          }
        ]
      }
    ],
    "graph": {
      "conflicts_with_edges": [],
      "metadata": {
        "num_clusters": "1",
        "num_records": "2",
        "timestamp": "2026-01-01T19:32:04.682956619+00:00",
        "version": "1.0"
      },
      "nodes": [
        {
          "cluster_id": null,
          "id": "record_2",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "crm_system",
            "uid": "2"
          },
          "record_id": 2
        },
        {
          "cluster_id": null,
          "id": "record_1",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 1
        },
        {
          "cluster_id": 0,
          "id": "cluster_0",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "GB00-VX",
            "cluster_key_identity": "preset",
            "golden": "[{\"attr\":\"country\",\"value\":\"GB\",\"interval\":{\"start\":851990400,\"end\":1482451200}},{\"attr\":\"currency\",\"value\":\"GBP\",\"interval\":{\"start\":851990400,\"end\":1482451200}},{\"attr\":\"department\",\"value\":\"XLON\",\"interval\":{\"start\":1019520000,\"end\":1022889600}},{\"attr\":\"department\",\"value\":\"XLON\",\"interval\":{\"start\":1363564800,\"end\":1482451200}},{\"attr\":\"email\",\"value\":\"john.doe@company.com\",\"interval\":{\"start\":851990400,\"end\":1482451200}},{\"attr\":\"employee_id\",\"value\":\"X7W5YMBX4\",\"interval\":{\"start\":851990400,\"end\":1482451200}},{\"attr\":\"phone\",\"value\":\"64541\",\"interval\":{\"start\":946684800,\"end\":1022889600}},{\"attr\":\"type\",\"value\":\"COMMON STOCK-S\",\"interval\":{\"start\":851990400,\"end\":980985600}},{\"attr\":\"type\",\"value\":\"COMMON STOCK-S\",\"interval\":{\"start\":1022889600,\"end\":1482451200}}]",
            "root_record": "R1",
            "size": "2"
          },
          "record_id": null
        }
      ],
      "same_as_edges": [
        {
          "interval": {
            "end": 9223372036854775807,
            "start": -9223372036854775808
          },
          "metadata": {},
          "reason": "cluster_member",
          "source": 2,
          "target": 1
        }
      ]
    }
  },
  {
    "id": "test_can_detect_intra_entity_conflicts",
    "label": "Intra-entity SSN conflicts (10 records)",
    "identityKeys": [],
    "conflictAttrs": [
      "ssn"
    ],
    "description": "Each record contains two overlapping SSNs. Expect red self-loop conflicts on every record card in the graph.",
    "records": [
      {
        "id": "R1",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "ssn",
            "value": "123-45-6789",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "ssn",
            "value": "987-65-4321",
            "start": 86400,
            "end": 432000
          }
        ]
      },
      {
        "id": "R2",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "ssn",
            "value": "123-45-6789",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "ssn",
            "value": "987-65-4321",
            "start": 86400,
            "end": 432000
          }
        ]
      },
      {
        "id": "R3",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "ssn",
            "value": "123-45-6789",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "ssn",
            "value": "987-65-4321",
            "start": 86400,
            "end": 432000
          }
        ]
      },
      {
        "id": "R4",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "ssn",
            "value": "123-45-6789",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "ssn",
            "value": "987-65-4321",
            "start": 86400,
            "end": 432000
          }
        ]
      },
      {
        "id": "R5",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "ssn",
            "value": "123-45-6789",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "ssn",
            "value": "987-65-4321",
            "start": 86400,
            "end": 432000
          }
        ]
      },
      {
        "id": "R6",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "ssn",
            "value": "123-45-6789",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "ssn",
            "value": "987-65-4321",
            "start": 86400,
            "end": 432000
          }
        ]
      },
      {
        "id": "R7",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "ssn",
            "value": "123-45-6789",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "ssn",
            "value": "987-65-4321",
            "start": 86400,
            "end": 432000
          }
        ]
      },
      {
        "id": "R8",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "ssn",
            "value": "123-45-6789",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "ssn",
            "value": "987-65-4321",
            "start": 86400,
            "end": 432000
          }
        ]
      },
      {
        "id": "R9",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "ssn",
            "value": "123-45-6789",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "ssn",
            "value": "987-65-4321",
            "start": 86400,
            "end": 432000
          }
        ]
      },
      {
        "id": "R10",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "ssn",
            "value": "123-45-6789",
            "start": 86400,
            "end": 432000
          },
          {
            "attr": "ssn",
            "value": "987-65-4321",
            "start": 86400,
            "end": 432000
          }
        ]
      }
    ],
    "graph": {
      "conflicts_with_edges": [],
      "metadata": {
        "num_clusters": "10",
        "num_records": "10",
        "timestamp": "2026-01-01T19:32:04.683093984+00:00",
        "version": "1.0"
      },
      "nodes": [
        {
          "cluster_id": null,
          "id": "record_1",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 1
        },
        {
          "cluster_id": null,
          "id": "record_5",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 5
        },
        {
          "cluster_id": null,
          "id": "record_7",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 7
        },
        {
          "cluster_id": null,
          "id": "record_8",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 8
        },
        {
          "cluster_id": null,
          "id": "record_4",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 4
        },
        {
          "cluster_id": null,
          "id": "record_6",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 6
        },
        {
          "cluster_id": null,
          "id": "record_2",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 2
        },
        {
          "cluster_id": null,
          "id": "record_10",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 10
        },
        {
          "cluster_id": null,
          "id": "record_9",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 9
        },
        {
          "cluster_id": null,
          "id": "record_3",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 3
        },
        {
          "cluster_id": 0,
          "id": "cluster_0",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "789-V9",
            "cluster_key_identity": "fallback_ssn",
            "golden": "[]",
            "root_record": "R8",
            "size": "1"
          },
          "record_id": null
        },
        {
          "cluster_id": 1,
          "id": "cluster_1",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "789-V9",
            "cluster_key_identity": "fallback_ssn",
            "golden": "[]",
            "root_record": "R10",
            "size": "1"
          },
          "record_id": null
        },
        {
          "cluster_id": 2,
          "id": "cluster_2",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "789-V9",
            "cluster_key_identity": "fallback_ssn",
            "golden": "[]",
            "root_record": "R2",
            "size": "1"
          },
          "record_id": null
        },
        {
          "cluster_id": 3,
          "id": "cluster_3",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "789-V9",
            "cluster_key_identity": "fallback_ssn",
            "golden": "[]",
            "root_record": "R1",
            "size": "1"
          },
          "record_id": null
        },
        {
          "cluster_id": 4,
          "id": "cluster_4",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "789-V9",
            "cluster_key_identity": "fallback_ssn",
            "golden": "[]",
            "root_record": "R3",
            "size": "1"
          },
          "record_id": null
        },
        {
          "cluster_id": 5,
          "id": "cluster_5",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "789-V9",
            "cluster_key_identity": "fallback_ssn",
            "golden": "[]",
            "root_record": "R4",
            "size": "1"
          },
          "record_id": null
        },
        {
          "cluster_id": 6,
          "id": "cluster_6",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "789-V9",
            "cluster_key_identity": "fallback_ssn",
            "golden": "[]",
            "root_record": "R9",
            "size": "1"
          },
          "record_id": null
        },
        {
          "cluster_id": 7,
          "id": "cluster_7",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "789-V9",
            "cluster_key_identity": "fallback_ssn",
            "golden": "[]",
            "root_record": "R5",
            "size": "1"
          },
          "record_id": null
        },
        {
          "cluster_id": 8,
          "id": "cluster_8",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "789-V9",
            "cluster_key_identity": "fallback_ssn",
            "golden": "[]",
            "root_record": "R7",
            "size": "1"
          },
          "record_id": null
        },
        {
          "cluster_id": 9,
          "id": "cluster_9",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "789-V9",
            "cluster_key_identity": "fallback_ssn",
            "golden": "[]",
            "root_record": "R6",
            "size": "1"
          },
          "record_id": null
        }
      ],
      "same_as_edges": []
    }
  },
  {
    "id": "test_can_detect_cross_entity_conflicts",
    "label": "Cross-entity email conflicts (same perspective)",
    "identityKeys": [],
    "conflictAttrs": [
      "email"
    ],
    "description": "Two records in the same perspective share the same email. Expect a red conflict edge even without a same-as link.",
    "records": [
      {
        "id": "R1",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "email",
            "value": "john.doe@company.com",
            "start": 86400,
            "end": 432000
          }
        ]
      },
      {
        "id": "R2",
        "uid": "2",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "email",
            "value": "john.doe@company.com",
            "start": 86400,
            "end": 432000
          }
        ]
      }
    ],
    "graph": {
      "conflicts_with_edges": [],
      "metadata": {
        "num_clusters": "2",
        "num_records": "2",
        "timestamp": "2026-01-01T19:32:04.683296052+00:00",
        "version": "1.0"
      },
      "nodes": [
        {
          "cluster_id": null,
          "id": "record_1",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 1
        },
        {
          "cluster_id": null,
          "id": "record_2",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "2"
          },
          "record_id": 2
        },
        {
          "cluster_id": 0,
          "id": "cluster_0",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "JD-HJ",
            "cluster_key_identity": "fallback_email",
            "golden": "[{\"attr\":\"email\",\"value\":\"john.doe@company.com\",\"interval\":{\"start\":86400,\"end\":432000}}]",
            "root_record": "R1",
            "size": "1"
          },
          "record_id": null
        },
        {
          "cluster_id": 1,
          "id": "cluster_1",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "JD-SB",
            "cluster_key_identity": "fallback_email",
            "golden": "[{\"attr\":\"email\",\"value\":\"john.doe@company.com\",\"interval\":{\"start\":86400,\"end\":432000}}]",
            "root_record": "R2",
            "size": "1"
          },
          "record_id": null
        }
      ],
      "same_as_edges": []
    }
  },
  {
    "id": "test_cross_entity_perspective_grouping",
    "label": "Cross-entity email (different perspectives)",
    "identityKeys": [],
    "conflictAttrs": [
      "email"
    ],
    "description": "Two records share the same email but sit in different perspectives. Expect no red conflict edge because uniqueness is scoped per perspective.",
    "records": [
      {
        "id": "R1",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "email",
            "value": "john.doe@company.com",
            "start": 86400,
            "end": 432000
          }
        ]
      },
      {
        "id": "R2",
        "uid": "2",
        "entityType": "person",
        "perspective": "crm_system",
        "descriptors": [
          {
            "attr": "email",
            "value": "john.doe@company.com",
            "start": 86400,
            "end": 432000
          }
        ]
      }
    ],
    "graph": {
      "conflicts_with_edges": [],
      "metadata": {
        "num_clusters": "2",
        "num_records": "2",
        "timestamp": "2026-01-01T19:32:04.683351716+00:00",
        "version": "1.0"
      },
      "nodes": [
        {
          "cluster_id": null,
          "id": "record_2",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "crm_system",
            "uid": "2"
          },
          "record_id": 2
        },
        {
          "cluster_id": null,
          "id": "record_1",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 1
        },
        {
          "cluster_id": 0,
          "id": "cluster_0",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "JD-HJ",
            "cluster_key_identity": "fallback_email",
            "golden": "[{\"attr\":\"email\",\"value\":\"john.doe@company.com\",\"interval\":{\"start\":86400,\"end\":432000}}]",
            "root_record": "R1",
            "size": "1"
          },
          "record_id": null
        },
        {
          "cluster_id": 1,
          "id": "cluster_1",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "JD-3K",
            "cluster_key_identity": "fallback_email",
            "golden": "[{\"attr\":\"email\",\"value\":\"john.doe@company.com\",\"interval\":{\"start\":86400,\"end\":432000}}]",
            "root_record": "R2",
            "size": "1"
          },
          "record_id": null
        }
      ],
      "same_as_edges": []
    }
  },
  {
    "id": "test_can_resolve_extend_descriptor_start_dating_for_national_rv_holding_edge_case",
    "label": "National RV holding edge case (descriptor start dating)",
    "identityKeys": [
      "ssn",
      "country"
    ],
    "conflictAttrs": [
      "employee_id",
      "phone",
      "email"
    ],
    "description": "Two records overlap only on a short SSN window. Expect a narrow same-as link with no red conflicts, highlighting the shorter attribute intervals.",
    "records": [
      {
        "id": "R1",
        "uid": "1",
        "entityType": "person",
        "perspective": "hr_system",
        "descriptors": [
          {
            "attr": "employee_id",
            "value": "D61Y3AJC7",
            "start": 851990400,
            "end": 1283040000
          },
          {
            "attr": "country",
            "value": "US",
            "start": 851990400,
            "end": 1283040000
          },
          {
            "attr": "currency",
            "value": "USD",
            "start": 851990400,
            "end": 1283040000
          },
          {
            "attr": "ssn",
            "value": "637277104",
            "start": 851990400,
            "end": 1283040000
          },
          {
            "attr": "name",
            "value": "US6372771047",
            "start": 851990400,
            "end": 1283040000
          },
          {
            "attr": "email",
            "value": "jane.smith@company.com",
            "start": 851990400,
            "end": 1283040000
          }
        ]
      },
      {
        "id": "R2",
        "uid": "2",
        "entityType": "person",
        "perspective": "crm_system",
        "descriptors": [
          {
            "attr": "phone",
            "value": "63819",
            "start": 899078400,
            "end": 1022889600
          },
          {
            "attr": "country",
            "value": "US",
            "start": 899078400,
            "end": 1022889600
          },
          {
            "attr": "currency",
            "value": "USD",
            "start": 899078400,
            "end": 1022889600
          },
          {
            "attr": "ssn",
            "value": "637277104",
            "start": 1022803200,
            "end": 1022889600
          },
          {
            "attr": "name",
            "value": "US6372771047",
            "start": 1022803200,
            "end": 1022889600
          },
          {
            "attr": "email",
            "value": "jane.smith@company.com",
            "start": 1022803200,
            "end": 1022889600
          }
        ]
      }
    ],
    "graph": {
      "conflicts_with_edges": [],
      "metadata": {
        "num_clusters": "1",
        "num_records": "2",
        "timestamp": "2026-01-01T19:32:04.683428359+00:00",
        "version": "1.0"
      },
      "nodes": [
        {
          "cluster_id": null,
          "id": "record_2",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "crm_system",
            "uid": "2"
          },
          "record_id": 2
        },
        {
          "cluster_id": null,
          "id": "record_1",
          "node_type": "record",
          "properties": {
            "entity_type": "person",
            "perspective": "hr_system",
            "uid": "1"
          },
          "record_id": 1
        },
        {
          "cluster_id": 0,
          "id": "cluster_0",
          "node_type": "cluster",
          "properties": {
            "cluster_key": "104-QZ",
            "cluster_key_identity": "preset",
            "golden": "[{\"attr\":\"country\",\"value\":\"US\",\"interval\":{\"start\":851990400,\"end\":1283040000}},{\"attr\":\"currency\",\"value\":\"USD\",\"interval\":{\"start\":851990400,\"end\":1283040000}},{\"attr\":\"email\",\"value\":\"jane.smith@company.com\",\"interval\":{\"start\":851990400,\"end\":1283040000}},{\"attr\":\"employee_id\",\"value\":\"D61Y3AJC7\",\"interval\":{\"start\":851990400,\"end\":1283040000}},{\"attr\":\"name\",\"value\":\"US6372771047\",\"interval\":{\"start\":851990400,\"end\":1283040000}},{\"attr\":\"phone\",\"value\":\"63819\",\"interval\":{\"start\":899078400,\"end\":1022889600}},{\"attr\":\"ssn\",\"value\":\"637277104\",\"interval\":{\"start\":851990400,\"end\":1283040000}}]",
            "root_record": "R1",
            "size": "2"
          },
          "record_id": null
        }
      ],
      "same_as_edges": [
        {
          "interval": {
            "end": 9223372036854775807,
            "start": -9223372036854775808
          },
          "metadata": {},
          "reason": "cluster_member",
          "source": 2,
          "target": 1
        }
      ]
    }
  }
];

async function loadPresetCatalog() {
  try {
    const response = await fetch("presets.json", { cache: "no-store" });
    if (!response.ok) return;
    const presets = await response.json();
    if (!Array.isArray(presets)) return;
    recordPresets.length = 0;
    presets.forEach((preset) => recordPresets.push(preset));
  } catch (_) {
    // Ignore fetch errors when running from file://
  }
}

tabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    tabs.forEach((t) => t.classList.remove("active"));
    tab.classList.add("active");
    showTab(tab.dataset.tab);
  });
});

function showTab(name) {
  const panes = {
    summary: debugSummary,
    records: debugRecords,
    graph: debugGraph,
    events: debugEvents,
    presets: debugPresets,
  };

  Object.entries(panes).forEach(([key, pane]) => {
    if (key === name) {
      pane.classList.remove("hidden");
    } else {
      pane.classList.add("hidden");
    }
  });
}

function logEvent(message) {
  const timestamp = new Date().toISOString();
  state.events.unshift(`[${timestamp}] ${message}`);
  if (state.events.length > 200) {
    state.events.pop();
  }
}

function showToast(message) {
  if (!toast) return;
  toast.textContent = message;
  toast.classList.add("show");
  setTimeout(() => {
    toast.classList.remove("show");
  }, 1400);
}

function setGraphDescription(text) {
  if (!graphDescription) return;
  graphDescription.textContent =
    text || "Load a preset to see a guided interpretation of the graph.";
}

function pulseButton(button) {
  if (!button) return;
  button.classList.remove("pulse");
  void button.offsetWidth;
  button.classList.add("pulse");
}

function createRecord() {
  return {
    id: `R${state.records.length + 1}`,
    uid: `uid_${state.records.length + 1}`,
    entityType: "person",
    perspective: "crm",
    descriptors: [],
  };
}

function createDescriptor() {
  return {
    attr: "email",
    value: "alice@example.com",
    start: 0,
    end: 10,
  };
}

function addRecord() {
  const record = createRecord();
  record.descriptors.push(createDescriptor());
  state.records.push(record);
  logEvent(`Added record ${record.id}`);
  renderRecords();
  renderGraph();
}

function removeRecord(index) {
  const [removed] = state.records.splice(index, 1);
  logEvent(`Removed record ${removed.id}`);
  renderRecords();
  renderGraph();
}

function parseList(input) {
  return input
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function applyPreset(key) {
  const preset = ontologyPresets[key];
  if (!preset) return;
  identityKeysInput.value = preset.identityKeys.join(", ");
  conflictAttrsInput.value = preset.conflictAttrs.join(", ");
  logEvent(`Applied ontology preset: ${key.toUpperCase()}`);
  renderGraph();
}

function applyPresetSettings(preset) {
  if (!preset) return;
  if (Array.isArray(preset.identityKeys)) {
    identityKeysInput.value = preset.identityKeys.join(", ");
  }
  if (Array.isArray(preset.conflictAttrs)) {
    conflictAttrsInput.value = preset.conflictAttrs.join(", ");
  }
  if (preset.description) {
    setGraphDescription(preset.description);
  }
}

function loadRecords(records, mode) {
  if (mode === "replace") {
    state.records = [];
  }
  state.graph = null;
  records.forEach((record) => {
    state.records.push({
      id: record.id,
      uid: record.uid,
      entityType: record.entityType,
      perspective: record.perspective,
      descriptors: record.descriptors.map((desc) => ({
        attr: desc.attr,
        value: desc.value,
        start: Number(desc.start),
        end: Number(desc.end),
      })),
    });
  });
  viewStartInput.value = "0";
  viewEndInput.value = "Infinity";
  logEvent(`${mode === "replace" ? "Loaded" : "Appended"} ${records.length} record(s)`);
  renderRecords();
  renderGraph();
}

function computeRangeFromRecords(records) {
  let minStart = null;
  let maxEnd = null;
  records.forEach((record) => {
    record.descriptors.forEach((descriptor) => {
      const start = Number(descriptor.start);
      const end = Number(descriptor.end);
      if (!Number.isFinite(start) || !Number.isFinite(end)) return;
      minStart = minStart === null ? start : Math.min(minStart, start);
      maxEnd = maxEnd === null ? end : Math.max(maxEnd, end);
    });
  });
  if (minStart === null || maxEnd === null) return null;
  if (minStart === maxEnd) {
    return { start: minStart, end: minStart + 1 };
  }
  return { start: minStart, end: maxEnd };
}

function computeRangeFromGolden(graph) {
  if (!graph || !Array.isArray(graph.nodes)) return null;
  let minStart = null;
  let maxEnd = null;
  graph.nodes.forEach((node) => {
    if (node.node_type !== "cluster") return;
    const goldenJson = node.properties && node.properties.golden;
    if (!goldenJson) return;
    try {
      const entries = JSON.parse(goldenJson);
      if (!Array.isArray(entries)) return;
      entries.forEach((entry) => {
        if (!entry || !entry.interval) return;
        const start = Number(entry.interval.start);
        const end = Number(entry.interval.end);
        if (!Number.isFinite(start) || !Number.isFinite(end)) return;
        minStart = minStart === null ? start : Math.min(minStart, start);
        maxEnd = maxEnd === null ? end : Math.max(maxEnd, end);
      });
    } catch (_) {
      // Ignore malformed golden data.
    }
  });
  if (minStart === null || maxEnd === null) return null;
  if (minStart === maxEnd) {
    return { start: minStart, end: minStart + 1 };
  }
  return { start: minStart, end: maxEnd };
}

function parseRange() {
  return { start: 0, end: Number.POSITIVE_INFINITY };
}

function renderRecords() {
  recordList.innerHTML = "";

  state.records.forEach((record, index) => {
    const card = recordTemplate.content.cloneNode(true);
    const article = card.querySelector(".record-card");

    const idInput = article.querySelector(".record-id");
    const uidInput = article.querySelector(".record-uid");
    const entityInput = article.querySelector(".record-entity");
    const perspectiveInput = article.querySelector(".record-perspective");

    idInput.value = record.id;
    uidInput.value = record.uid;
    entityInput.value = record.entityType;
    perspectiveInput.value = record.perspective;

    idInput.addEventListener("input", (event) => {
      record.id = event.target.value;
      renderGraph();
    });
    uidInput.addEventListener("input", (event) => {
      record.uid = event.target.value;
      renderGraph();
    });
    entityInput.addEventListener("input", (event) => {
      record.entityType = event.target.value;
      renderGraph();
    });
    perspectiveInput.addEventListener("input", (event) => {
      record.perspective = event.target.value;
      renderGraph();
    });

    const descriptorList = article.querySelector(".descriptor-list");
    record.descriptors.forEach((descriptor, descriptorIndex) => {
      const row = descriptorTemplate.content.cloneNode(true);
      const attrInput = row.querySelector(".descriptor-attr");
      const valueInput = row.querySelector(".descriptor-value");
      const startInput = row.querySelector(".descriptor-start");
      const endInput = row.querySelector(".descriptor-end");
      const removeButton = row.querySelector(".remove-descriptor");

      attrInput.value = descriptor.attr;
      valueInput.value = descriptor.value;
      startInput.value = descriptor.start;
      endInput.value = descriptor.end;

      attrInput.addEventListener("input", (event) => {
        descriptor.attr = event.target.value;
        renderGraph();
      });
      valueInput.addEventListener("input", (event) => {
        descriptor.value = event.target.value;
        renderGraph();
      });
      startInput.addEventListener("input", (event) => {
        descriptor.start = Number(event.target.value);
        renderGraph();
      });
      endInput.addEventListener("input", (event) => {
        descriptor.end = Number(event.target.value);
        renderGraph();
      });

      removeButton.addEventListener("click", () => {
        record.descriptors.splice(descriptorIndex, 1);
        logEvent(`Removed descriptor from ${record.id}`);
        renderRecords();
        renderGraph();
      });

      descriptorList.appendChild(row);
    });

    const addDescriptorButton = article.querySelector(".add-descriptor");
    addDescriptorButton.addEventListener("click", () => {
      record.descriptors.push(createDescriptor());
      logEvent(`Added descriptor to ${record.id}`);
      renderRecords();
      renderGraph();
    });

    article.querySelector(".remove-record").addEventListener("click", () => {
      removeRecord(index);
    });

    recordList.appendChild(card);
  });
}

function overlapping(a, b) {
  return a.start < b.end && b.start < a.end;
}

function intersect(a, b) {
  const start = Math.max(a.start, b.start);
  const end = Math.min(a.end, b.end);
  if (start < end) {
    return { start, end };
  }
  return null;
}

function coalesce(intervals) {
  if (!intervals.length) return [];
  const sorted = [...intervals].sort((a, b) => a.start - b.start);
  const result = [sorted[0]];
  for (let i = 1; i < sorted.length; i += 1) {
    const last = result[result.length - 1];
    const current = sorted[i];
    if (current.start <= last.end) {
      last.end = Math.max(last.end, current.end);
    } else {
      result.push({ ...current });
    }
  }
  return result;
}


function buildSameAsEdges(records, identityAttrs) {
  const edges = [];
  for (let i = 0; i < records.length; i += 1) {
    for (let j = i + 1; j < records.length; j += 1) {
      const recordA = records[i];
      const recordB = records[j];

      let candidateIntervals = null;
      let allMatched = true;

      identityAttrs.forEach((attr) => {
        const descA = recordA.descriptors.filter((d) => d.attr === attr);
        const descB = recordB.descriptors.filter((d) => d.attr === attr);
        const overlaps = [];

        descA.forEach((a) => {
          descB.forEach((b) => {
            if (a.value === b.value) {
              const overlap = intersect(a, b);
              if (overlap) {
                overlaps.push(overlap);
              }
            }
          });
        });

        if (!overlaps.length) {
          allMatched = false;
        } else if (candidateIntervals === null) {
          candidateIntervals = overlaps;
        } else {
          const next = [];
          candidateIntervals.forEach((intervalA) => {
            overlaps.forEach((intervalB) => {
              const overlap = intersect(intervalA, intervalB);
              if (overlap) {
                next.push(overlap);
              }
            });
          });
          candidateIntervals = next;
        }
      });

      if (allMatched && candidateIntervals && candidateIntervals.length) {
        const merged = coalesce(candidateIntervals);
        merged.forEach((interval) => {
          edges.push({
            source: recordA.id,
            target: recordB.id,
            interval,
            reason: "identity_key",
          });
        });
      }
    }
  }
  return edges;
}

function buildClusters(records, sameAsEdges) {
  const parent = new Map();
  records.forEach((record) => parent.set(record.id, record.id));

  function find(id) {
    const root = parent.get(id);
    if (root === id) return root;
    const next = find(root);
    parent.set(id, next);
    return next;
  }

  function union(a, b) {
    const rootA = find(a);
    const rootB = find(b);
    if (rootA !== rootB) {
      parent.set(rootB, rootA);
    }
  }

  sameAsEdges.forEach((edge) => union(edge.source, edge.target));

  const clusters = new Map();
  records.forEach((record) => {
    const root = find(record.id);
    if (!clusters.has(root)) {
      clusters.set(root, []);
    }
    clusters.get(root).push(record.id);
  });

  return Array.from(clusters.entries()).map(([root, members], index) => ({
    id: `C${index + 1}`,
    root,
    records: members,
  }));
}

function buildConflictEdges(records, clusters, conflictAttrs) {
  const edges = [];
  const seen = new Set();
  const recordMap = new Map(records.map((r) => [r.id, r]));

  function addEdge(edge) {
    const key = [
      edge.kind,
      edge.attr,
      edge.source,
      edge.target,
      edge.interval.start,
      edge.interval.end,
      edge.values.join("|"),
    ].join("::");
    if (seen.has(key)) return;
    seen.add(key);
    edges.push(edge);
  }

  clusters.forEach((cluster) => {
    const members = cluster.records;
    for (let i = 0; i < members.length; i += 1) {
      for (let j = i + 1; j < members.length; j += 1) {
        const recordA = recordMap.get(members[i]);
        const recordB = recordMap.get(members[j]);
        conflictAttrs.forEach((attr) => {
          recordA.descriptors
            .filter((d) => d.attr === attr)
            .forEach((descA) => {
              recordB.descriptors
                .filter((d) => d.attr === attr)
                .forEach((descB) => {
                  if (descA.value !== descB.value) {
                    const overlap = intersect(descA, descB);
                    if (overlap) {
                      addEdge({
                        source: recordA.id,
                        target: recordB.id,
                        attr,
                        interval: overlap,
                        values: [descA.value, descB.value],
                        kind: "mismatch",
                      });
                    }
                  }
                });
            });
        });
      }
    }
  });

  conflictAttrs.forEach((attr) => {
    const byPerspective = new Map();
    records.forEach((record) => {
      const perspective = record.perspective || "default";
      const descriptors = record.descriptors.filter((d) => d.attr === attr);
      if (!descriptors.length) return;
      if (!byPerspective.has(perspective)) {
        byPerspective.set(perspective, new Map());
      }
      const byValue = byPerspective.get(perspective);
      descriptors.forEach((descriptor) => {
        if (!byValue.has(descriptor.value)) {
          byValue.set(descriptor.value, []);
        }
        byValue.get(descriptor.value).push({ recordId: record.id, descriptor });
      });
    });

    byPerspective.forEach((byValue) => {
      byValue.forEach((entries, value) => {
        for (let i = 0; i < entries.length; i += 1) {
          for (let j = i + 1; j < entries.length; j += 1) {
            const a = entries[i];
            const b = entries[j];
            if (a.recordId === b.recordId) continue;
            const overlap = intersect(a.descriptor, b.descriptor);
            if (!overlap) continue;
            const [source, target] = a.recordId < b.recordId ? [a.recordId, b.recordId] : [b.recordId, a.recordId];
            addEdge({
              source,
              target,
              attr,
              interval: overlap,
              values: [value],
              kind: "duplicate",
            });
          }
        }
      });
    });
  });

  conflictAttrs.forEach((attr) => {
    records.forEach((record) => {
      const descriptors = record.descriptors.filter((d) => d.attr === attr);
      if (descriptors.length < 2) return;
      for (let i = 0; i < descriptors.length; i += 1) {
        for (let j = i + 1; j < descriptors.length; j += 1) {
          const descA = descriptors[i];
          const descB = descriptors[j];
          if (descA.value === descB.value) continue;
          const overlap = intersect(descA, descB);
          if (!overlap) continue;
          addEdge({
            source: record.id,
            target: record.id,
            attr,
            interval: overlap,
            values: [descA.value, descB.value],
            kind: "intra",
          });
        }
      }
    });
  });

  return edges;
}


function applyViewRange(edges, range) {
  return edges.filter((edge) => overlapping(edge.interval, range));
}

function updateStats(clusters, conflictEdges) {
  statRecords.textContent = state.records.length;
  statClusters.textContent = clusters.length;
  statConflicts.textContent = conflictEdges.length;
}

function renderGraph() {
  const identityAttrs = parseList(identityKeysInput.value);
  const conflictAttrs = parseList(conflictAttrsInput.value);
  const viewRange = parseRange();

  const sameAsEdges = buildSameAsEdges(state.records, identityAttrs);
  const clusters = buildClusters(state.records, sameAsEdges);
  const conflictEdges = buildConflictEdges(state.records, clusters, conflictAttrs);
  const goldenAttributes = buildGoldenFromGraph(state.graph);
  const clusterKeyByRoot = buildClusterKeyByRoot(state.graph);
  const clusterKeyById = buildClusterKeyById(state.graph);

  clusters.forEach((cluster) => {
    const key = clusterKeyByRoot.get(cluster.root);
    if (key) {
      cluster.key = key.value;
      cluster.keyIdentity = key.identity;
    }
  });

  const filteredSameAs = applyViewRange(sameAsEdges, viewRange);
  const filteredConflicts = applyViewRange(conflictEdges, viewRange);

  updateStats(clusters, filteredConflicts);
  renderCytoscape(clusters, state.records, filteredSameAs, filteredConflicts);
  renderDebug(clusters, filteredSameAs, filteredConflicts, viewRange);
  renderGoldenSummary(
    clusters,
    goldenAttributes,
    viewRange,
    clusterKeyById,
    Boolean(state.graph),
  );
}

function formatInterval(interval) {
  if (!interval) return "";
  return `${interval.start}–${interval.end}`;
}

function hashAttr(attr) {
  let hash = 0;
  for (let i = 0; i < attr.length; i += 1) {
    hash = (hash << 5) - hash + attr.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

function colorForAttr(attr) {
  const palette = ["#38bdf8", "#f97316", "#a78bfa", "#22c55e", "#fbbf24", "#ec4899"];
  return palette[hashAttr(attr) % palette.length];
}

function buildDescriptorNodes(records) {
  const descriptorMap = new Map();
  records.forEach((record) => {
    const descriptors = record.descriptors || [];
    descriptors.forEach((descriptor) => {
      const interval = { start: descriptor.start, end: descriptor.end };
      const key = `${descriptor.attr}::${descriptor.value}`;
      if (!descriptorMap.has(key)) {
        descriptorMap.set(key, {
          id: `desc_${descriptorMap.size + 1}`,
          attr: descriptor.attr,
          value: descriptor.value,
          intervals: [],
          recordIds: new Set(),
        });
      }
      const node = descriptorMap.get(key);
      node.intervals.push(interval);
      node.recordIds.add(record.id);
    });
  });

  descriptorMap.forEach((node) => {
    node.intervals = coalesce(node.intervals);
  });

  return Array.from(descriptorMap.values()).sort((a, b) => {
    const attrCompare = a.attr.localeCompare(b.attr);
    if (attrCompare !== 0) return attrCompare;
    const valueCompare = String(a.value).localeCompare(String(b.value));
    if (valueCompare !== 0) return valueCompare;
    const aStart = a.intervals[0]?.start ?? 0;
    const bStart = b.intervals[0]?.start ?? 0;
    return aStart - bStart;
  });
}

function renderCytoscape(clusters, records, sameAsEdges, conflictEdges) {
  if (!graphContainer || typeof cytoscape === "undefined") return;

  const descriptorNodes = buildDescriptorNodes(records);

  const elements = [];
  clusters.forEach((cluster) => {
    const label = cluster.key
      ? `${cluster.key}\n${cluster.id}`
      : `Cluster ${cluster.id.replace("C", "")}`;
    elements.push({
      data: {
        id: cluster.id,
        label,
        type: "cluster",
      },
    });
  });

  records.forEach((record) => {
    elements.push({
      data: {
        id: record.id,
        label: `${record.id}\n${record.entityType} • ${record.perspective}`,
        perspective: record.perspective,
        type: "record",
      },
    });
  });

  descriptorNodes.forEach((node) => {
    elements.push({
      data: {
        id: node.id,
        label: `${node.attr}: ${node.value}`,
        attr: node.attr,
        color: colorForAttr(node.attr),
        type: "descriptor",
        intervals: node.intervals || [],
      },
    });
  });

  clusters.forEach((cluster) => {
    cluster.records.forEach((recordId) => {
      elements.push({
        data: {
          id: `cluster-${cluster.id}-${recordId}`,
          source: cluster.id,
          target: recordId,
          type: "cluster-link",
        },
      });
    });
  });

  sameAsEdges.forEach((edge) => {
    elements.push({
      data: {
        id: `same-${edge.source}-${edge.target}-${edge.interval.start}-${edge.interval.end}`,
        source: edge.source,
        target: edge.target,
        label: formatInterval(edge.interval),
        type: "same",
      },
    });
  });

  conflictEdges.forEach((edge, index) => {
    elements.push({
      data: {
        id: `conflict-${edge.source}-${edge.target}-${edge.attr}-${edge.interval.start}-${edge.interval.end}-${index}`,
        source: edge.source,
        target: edge.target,
        label: formatInterval(edge.interval),
        type: "conflict",
        kind: edge.kind || "conflict",
        attr: edge.attr,
        values: edge.values || [],
        interval: edge.interval,
      },
    });
  });

  descriptorNodes.forEach((node) => {
    node.recordIds.forEach((recordId) => {
      const intervals = node.intervals || [];
      const label = intervals.length
        ? intervals.map((interval) => formatInterval(interval)).join(", ")
        : "";
      elements.push({
        data: {
          id: `desc-${recordId}-${node.id}`,
          source: recordId,
          target: node.id,
          label,
          type: "descriptor-edge",
        },
      });
    });
  });

  const layout = {
    name: "concentric",
    animate: false,
    minNodeSpacing: 20,
    concentric(node) {
      if (node.data("type") === "cluster") return 4;
      if (node.data("type") === "record") return 3;
      return 2;
    },
    levelWidth() {
      return 1;
    },
  };

  if (cy) {
    cy.destroy();
  }

  cy = cytoscape({
    container: graphContainer,
    elements,
    layout,
    style: [
      {
        selector: "node",
        style: {
          "font-family": "Space Grotesk, Segoe UI, sans-serif",
          "font-size": 9,
          color: "#0b0f10",
          "text-valign": "center",
          "text-halign": "center",
          "text-wrap": "wrap",
          "text-max-width": 120,
          "background-opacity": 0.92,
          "border-width": 1,
          "border-color": "rgba(255,255,255,0.15)",
          "shadow-blur": 8,
          "shadow-color": "rgba(6,10,12,0.4)",
          "shadow-opacity": 0.55,
        },
      },
      {
        selector: 'node[type = "cluster"]',
        style: {
          shape: "roundrectangle",
          width: 92,
          height: 36,
          "background-color": "#facc15",
          label: "data(label)",
          "font-size": 9,
        },
      },
      {
        selector: 'node[type = "record"]',
        style: {
          shape: "roundrectangle",
          width: 130,
          height: 44,
          "background-color": "#22d3ee",
          label: "data(label)",
          "font-size": 8.5,
        },
      },
      {
        selector: 'node[type = "descriptor"]',
        style: {
          shape: "roundrectangle",
          width: 150,
          height: 36,
          "background-color": "data(color)",
          label: "data(label)",
          color: "#0b0f10",
          "font-size": 8.5,
        },
      },
      {
        selector: "edge",
        style: {
          width: 1.6,
          "curve-style": "bezier",
          "line-color": "rgba(148,163,184,0.6)",
          "target-arrow-shape": "none",
          label: "data(label)",
          "font-size": 8,
          "text-background-color": "rgba(12,18,22,0.8)",
          "text-background-opacity": 1,
          "text-background-padding": 3,
          "text-background-shape": "roundrectangle",
          color: "#f5f7f4",
        },
      },
      {
        selector: 'edge[type = "cluster-link"]',
        style: {
          "line-style": "dashed",
          "line-color": "rgba(56,189,248,0.35)",
          width: 1,
          label: "",
        },
      },
      {
        selector: 'edge[type = "same"]',
        style: {
          "line-color": "rgba(52,211,153,0.9)",
          width: 2,
        },
      },
      {
        selector: 'edge[type = "conflict"]',
        style: {
          "line-color": "rgba(248,113,113,0.95)",
          "line-style": "dashed",
          width: 2.5,
        },
      },
      {
        selector: 'edge[type = "descriptor-edge"]',
        style: {
          "line-color": "rgba(167,139,250,0.6)",
          width: 1.5,
        },
      },
    ],
  });

  cy.on("mouseover", 'edge[type = "conflict"]', (event) => {
    const edge = event.target.data();
    showEdgeDetails(edge);
  });
  cy.on("mouseout", 'edge[type = "conflict"]', () => {
    clearEdgeDetails();
  });
}

function renderGoldenSummary(
  clusters,
  goldenAttributes,
  viewRange,
  clusterKeyById,
  graphLoaded,
) {
  if (!goldenSummary) return;
  goldenSummary.innerHTML = "";
  if (!goldenAttributes || goldenAttributes.size === 0) {
    const empty = document.createElement("p");
    empty.className = "golden-empty";
    empty.textContent = graphLoaded
      ? "Golden record has no conflict-free attributes for this graph."
      : "Golden record is available when a KnowledgeGraph JSON is loaded.";
    goldenSummary.appendChild(empty);
    return;
  }

  const hasGoldenValues = Array.from(goldenAttributes.values()).some(
    (items) => items && items.length > 0,
  );
  if (!hasGoldenValues) {
    const empty = document.createElement("p");
    empty.className = "golden-empty";
    empty.textContent = "Golden record has no conflict-free attributes for this graph.";
    goldenSummary.appendChild(empty);
    return;
  }

  const grid = document.createElement("div");
  grid.className = "golden-grid";

  const clusterOrder = Array.from(goldenAttributes.keys()).sort((a, b) => {
    const aNum = typeof a === "number" ? a : Number(String(a).replace(/\D+/g, ""));
    const bNum = typeof b === "number" ? b : Number(String(b).replace(/\D+/g, ""));
    if (Number.isFinite(aNum) && Number.isFinite(bNum)) {
      return aNum - bNum;
    }
    return String(a).localeCompare(String(b));
  });

  clusterOrder.forEach((clusterId) => {
    const items = (goldenAttributes.get(clusterId) || []).filter((item) =>
      overlapping(item.interval, viewRange),
    );
    if (!items.length) return;
    const card = document.createElement("div");
    card.className = "golden-card";

    const title = document.createElement("h4");
    const key = clusterKeyById ? clusterKeyById.get(clusterId) : null;
    title.textContent = key
      ? `Cluster ${clusterId} • ${key}`
      : `Cluster ${clusterId} Golden Record`;
    card.appendChild(title);

    items
      .sort((a, b) => a.attr.localeCompare(b.attr))
      .forEach((item) => {
        const row = document.createElement("div");
        row.className = "golden-item";
        const attr = document.createElement("strong");
        attr.textContent = item.attr;
        const value = document.createElement("span");
        value.textContent = item.value;
        const pill = document.createElement("span");
        pill.className = "golden-pill";
        pill.textContent = formatInterval(item.interval);
        row.appendChild(attr);
        row.appendChild(value);
        row.appendChild(pill);
        card.appendChild(row);
      });

    grid.appendChild(card);
  });

  if (!grid.children.length) {
    const empty = document.createElement("p");
    empty.className = "golden-empty";
    empty.textContent = "Golden record has no attributes in the current view range.";
    goldenSummary.appendChild(empty);
    return;
  }

  goldenSummary.appendChild(grid);
}

function buildGoldenFromGraph(graph) {
  if (!graph || !Array.isArray(graph.nodes)) {
    return new Map();
  }
  const golden = new Map();
  graph.nodes.forEach((node) => {
    if (node.node_type !== "cluster") return;
    const clusterId =
      node.cluster_id ??
      (typeof node.id === "string" && node.id.startsWith("cluster_")
        ? Number(node.id.replace("cluster_", ""))
        : node.id);
    const goldenJson = node.properties && node.properties.golden;
    if (!goldenJson) return;
    try {
      const parsed = JSON.parse(goldenJson);
      if (Array.isArray(parsed)) {
        golden.set(clusterId, parsed);
      }
    } catch (_) {
      // Ignore malformed golden data.
    }
  });
  return golden;
}

function buildClusterKeyByRoot(graph) {
  const keyMap = new Map();
  if (!graph || !Array.isArray(graph.nodes)) {
    return keyMap;
  }
  graph.nodes.forEach((node) => {
    if (node.node_type !== "cluster") return;
    const props = node.properties || {};
    if (!props.cluster_key || !props.root_record) return;
    keyMap.set(props.root_record, {
      value: props.cluster_key,
      identity: props.cluster_key_identity || "",
    });
  });
  return keyMap;
}

function buildClusterKeyById(graph) {
  const keyMap = new Map();
  if (!graph || !Array.isArray(graph.nodes)) {
    return keyMap;
  }
  graph.nodes.forEach((node) => {
    if (node.node_type !== "cluster") return;
    const props = node.properties || {};
    const clusterId =
      node.cluster_id ??
      (typeof node.id === "string" && node.id.startsWith("cluster_")
        ? Number(node.id.replace("cluster_", ""))
        : node.id);
    if (!props.cluster_key) return;
    keyMap.set(clusterId, props.cluster_key);
  });
  return keyMap;
}

function renderDebug(clusters, sameAsEdges, conflictEdges, viewRange) {
  const summary = {
    settings: {
      identityKeys: parseList(identityKeysInput.value),
      conflictAttrs: parseList(conflictAttrsInput.value),
      viewRange,
    },
    stats: {
      records: state.records.length,
      clusters: clusters.length,
      sameAsEdges: sameAsEdges.length,
      conflictEdges: conflictEdges.length,
    },
  };

  const graph = {
    clusters,
    sameAsEdges,
    conflictEdges,
  };

  debugSummary.textContent = JSON.stringify(summary, null, 2);
  debugRecords.textContent = JSON.stringify(state.records, null, 2);
  debugGraph.textContent = JSON.stringify(graph, null, 2);
  debugEvents.textContent = state.events.join("\n");
}

function showEdgeDetails(edge) {
  edgeDetails.querySelector(".edge-empty").classList.add("hidden");
  edgeDetails.querySelector(".edge-body").classList.remove("hidden");
  edgeType.textContent = edge.kind || "conflict";
  edgeAttr.textContent = edge.attr;
  edgeValues.textContent =
    edge.kind === "duplicate" ? edge.values[0] : edge.values.join(" vs ");
  edgeInterval.textContent = `[${edge.interval.start}, ${edge.interval.end})`;
  edgeRecords.textContent = `${edge.source} ↔ ${edge.target}`;
}

function clearEdgeDetails() {
  edgeDetails.querySelector(".edge-empty").classList.remove("hidden");
  edgeDetails.querySelector(".edge-body").classList.add("hidden");
  edgeType.textContent = "";
  edgeAttr.textContent = "";
  edgeValues.textContent = "";
  edgeInterval.textContent = "";
  edgeRecords.textContent = "";
}

copyDebugButton.addEventListener("click", async () => {
  const payload = {
    records: state.records,
    settings: {
      identityKeys: parseList(identityKeysInput.value),
      conflictAttrs: parseList(conflictAttrsInput.value),
      viewRange: parseRange(),
    },
  };
  try {
    await navigator.clipboard.writeText(JSON.stringify(payload, null, 2));
    copyDebugButton.textContent = "Copied!";
    setTimeout(() => {
      copyDebugButton.textContent = "Copy JSON";
    }, 1200);
  } catch (err) {
    console.error(err);
  }
});

addRecordButton.addEventListener("click", addRecord);
refreshButton.addEventListener("click", renderGraph);
applyPresetButton.addEventListener("click", () => {
  applyPreset(ontologyPresetSelect.value);
});

[identityKeysInput, conflictAttrsInput, viewStartInput, viewEndInput].forEach((input) => {
  input.addEventListener("input", () => {
    renderGraph();
  });
});

addRecord();
addRecord();
applyPreset("iam");
renderRecords();
renderGraph();

loadPresetCatalog().finally(() => {
  presetSelect.innerHTML = "";
  recordPresets.forEach((preset) => {
    const option = document.createElement("option");
    option.value = preset.id;
    option.textContent = preset.label;
    presetSelect.appendChild(option);
  });
});

loadPresetButton.addEventListener("click", () => {
  const preset = recordPresets.find((item) => item.id === presetSelect.value);
  if (preset) {
    applyPresetSettings(preset);
    loadRecords(preset.records, "replace");
    state.graph = preset.graph || null;
    viewStartInput.value = "0";
    viewEndInput.value = "Infinity";
    renderGraph();
    showToast(`Loaded preset: ${preset.label}`);
    pulseButton(loadPresetButton);
  }
});

appendPresetButton.addEventListener("click", () => {
  const preset = recordPresets.find((item) => item.id === presetSelect.value);
  if (preset) {
    applyPresetSettings(preset);
    loadRecords(preset.records, "append");
    state.graph = null;
    renderGraph();
    showToast(`Appended preset: ${preset.label}`);
    pulseButton(appendPresetButton);
  }
});

loadJsonButton.addEventListener("click", () => {
  if (!presetJsonInput.value.trim()) {
    return;
  }
  try {
    const parsed = JSON.parse(presetJsonInput.value);
    if (!Array.isArray(parsed)) {
      throw new Error("JSON must be an array of records");
    }
    loadRecords(parsed, "replace");
    setGraphDescription(
      "Custom dataset loaded. Inspect nodes and conflict edges against the current view range.",
    );
    showToast("Loaded JSON records");
  } catch (err) {
    console.error(err);
    logEvent(`Failed to load JSON: ${err.message}`);
    showToast("Failed to load JSON");
  }
});

loadGraphJsonButton.addEventListener("click", () => {
  if (!graphJsonInput.value.trim()) {
    return;
  }
  try {
    const parsed = JSON.parse(graphJsonInput.value);
    if (!parsed || !Array.isArray(parsed.nodes)) {
      throw new Error("Graph JSON must include nodes");
    }
    state.graph = parsed;
    viewStartInput.value = "0";
    viewEndInput.value = "Infinity";
    showToast("Loaded knowledge graph");
    renderGraph();
  } catch (err) {
    console.error(err);
    logEvent(`Failed to load graph JSON: ${err.message}`);
    showToast("Failed to load graph JSON");
  }
});

clearRecordsButton.addEventListener("click", () => {
  state.records = [];
  state.graph = null;
  logEvent("Cleared all records");
  renderRecords();
  renderGraph();
  setGraphDescription("No records loaded. Add or load data to populate the graph.");
  showToast("Cleared records");
  pulseButton(clearRecordsButton);
});
