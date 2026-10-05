# Topic ontology review — attempt-2026-10-05-11-54

Input: `topics-global.yml` (1027 topics) plus 293 `topics-of-course-*.yml` files. Per-course descriptions are identical to the global ones, so the only evidence per course is the course name, its cluster, and the other topics assigned to it. The candidates below come from course-usage counts, co-occurrence analysis, and description embeddings (`paraphrase-multilingual-mpnet-base-v2`, cosine ≥ 0.70, see *Method*). Every proposal was then judged by hand. The proposals are meant for discussion, not as final decisions.

## TL;DR

- **The main problem is mixed granularity in the same subject area.** About 25 "whole-subject" umbrella topics (`machine_learning`, `operating_systems`, `data_structures`, …) are used by 15–43 courses. Most of those courses *also* carry the fine-grained child topics: for example, `machine_learning` co-occurs with `supervised_unsupervised_learning`, `classification_algorithms` and `clustering`, and `operating_systems` with `processes_threads`, `cpu_scheduling` and `file_systems`. The umbrella topic then means one of two different things. In general-education ("literacy") courses it is a short overview. In specialist courses it is a duplicate of the children. For module building these are two different modules.
- **The same concept appears under language-specific keys.** `functions` ("Functions in Python", 18 courses, 16 of them Python) vs `functions_recursion` (16 courses, mostly C). `basic_data_types`, `scope_visibility` and `object_mutability` have Python-only descriptions but are generic concepts.
- **Some single courses were split into 10–17 singleton micro-topics.** Examples: Digital Forensic 17/19 singletons, IT & Game Localization 11/11, Quantum Computing 10/12, Open Science 9/12, Computing Education 8/10, DIGITAL HEALTH M 14/36. Meanwhile other courses get 2–4 coarse topics (Data Intensive Applications: `data_science`, `machine_learning`, `neural_networks_deep_learning`, `programming_in_python`). Topics per course range from 2 to 36 (median 12).
- The 95 merge groups and 23 splits proposed below give **1025 → 881 used topics** and **440 → 330 singletons**. Average topics per course goes from 12.9 to 12.2. Topics used by ≥ 10 courses go from 90 to 99. After the change, the most-shared topics (`computer_organization_basics` 43, `functions` 34, `algorithmic_problem_solving` 33, `builtin_collections` 33, …) are **homogeneous intro-level units**, not heterogeneous umbrellas. That is the property the module-partitioning step needs.

## Estimated effect

Simulated by applying the YAML at the end to the per-course topic sets (splits first, then merges). Course assignments for splits are best-effort.

| Metric | Before | After splits | After splits + merges |
|---|---|---|---|
| Topics in `topics-global.yml` | 1027 (2 unused) | — | ≈ 881 (+ drop 2 unused) |
| Topics used by ≥ 1 course | 1025 | 1048 | **881** |
| Singleton topics (1 course) | 440 | 440 | **330** |
| Topics used by ≥ 10 courses | 90 | 100 | 99 |
| Largest topic | `machine_learning` (43, heterogeneous) | `builtin_collections` (33) | `computer_organization_basics` (43, homogeneous intro) |
| Avg topics / course | 12.9 | 13.0 | 12.2 |

The singleton count is still high because many singletons are legitimate specialist content (for example the CV pipeline topics `hough_transform` and `stereo_vision`). §3 lists what else could be done.

---

## 1. Split candidates (ordered by impact)

The recurring pattern is **literacy/overview vs specialist depth**. Most umbrella topics are used both by general-education courses (Informatics for X, Basic IT Skills, Computer Science for Environmental Sciences, …) and by full courses on that subject. In the full courses, finer child topics already exist. The split therefore usually creates (a) an *overview* key for the literacy courses, and (b) a *core* key for the specialist courses, or just reuses the existing children. A sensible companion rule for the extraction prompt: **do not assign an umbrella topic when ≥ 2 of its children are assigned.**

Course lists are best-effort, inferred from course names and co-assigned topics. They are given in full in the YAML; only the evidence is summarized here.

| # | Topic (courses) | Proposed sub-topics (courses) | Evidence |
|---|---|---|---|
| 1 | `machine_learning` (43) | `ml_fundamentals` (19): learning problem, train/val/test, generalization, overfitting · `applied_ml_workflow` (10): end-to-end pipelines with libraries in a domain · `ml_literacy_overview` (14): what ML is, uses, limits | ML/DL/NLP/CV courses (81610, 91250*, 95631*, 91258, 91266…) also carry `supervised_unsupervised_learning`, `classification_algorithms`, `clustering`, `regression_analysis`. Applied courses (IoT 81683, Embedded Sensors B5722, Digital Health B8018, Operational Analytics 95638, AI in Industry 91261) carry domain topics plus `predictive_analytics`. Literacy-type users: Digital Culture 93379, Cybersecurity 93470-A/B5793, Informatics for the Humanities B4826, AI/blockchain B0074/B1459, AI survey courses 78778/81940/90147/B0069. Also merge `supervised_unsupervised_learning` (16, pure umbrella) into `ml_fundamentals`. |
| 2 | `neural_networks_deep_learning` (32) | `neural_network_fundamentals` (19): neurons, MLP, backprop, training (absorbs `feedforward_neural_networks`, `neural_network_training`) · `deep_learning_overview` (13) | DL/ML/NLP courses co-assign CNN/RNN/`generative_models`. Survey/applied users (AI 81940/90147/B0069, Industry 4.0 95639, Human Data Science 93469, CPS Programming 99195, Advanced Software Modelling B0009, Programming Laboratory B0063) have no training-related topics. |
| 3 | `data_structures` (42) | `builtin_collections` (33): lists/tuples/dicts/records in intro programming · `linear_data_structures` (12): linked lists, stacks, queues as ADTs · `tree_graph_data_structures` (7) | The description covers vectors…graphs. In intro-programming courses it co-occurs with `basic_data_types` (17) and `imperative_constructs` (22). The ADS courses (11929*, 37635, B4913, B5782, 85301) have `hash_tables`, `heaps_priority_queues` and `union_find_structures` beside it. |
| 4 | `algorithm_design_analysis` (40) | `algorithmic_problem_solving` (30): problem → algorithm, pseudocode (absorbs `computational_thinking`) · `algorithm_design_techniques` (10) | 30 of the 40 users are first-year or literacy programming courses. Only the ADS and theory courses (11929*, 37635, B4913, B5782, 85301, 11933/41169, 58437) do actual design/analysis, and they also carry `asymptotic_analysis`, `recursive_algorithms` and `dynamic_programming`. |
| 5 | `computer_hardware_architecture` (38) | `computer_organization_basics` (28): von Neumann, CPU/memory/I/O at overview level (absorbs `computer_operation` 16, `von_neumann_architecture` 10, `program_execution_model` 10) · `processor_system_architecture` (10) | Co-occurs with `computer_operation` in 12 courses and `von_neumann_architecture` in 8. Deep architecture courses (03716, 11925, 28011/28012, 69430, 69731, 91602) carry `cpu_organization`, `instruction_set_architecture` and `memory_hierarchy`. The others are Informatics/Basic IT Skills style. |
| 6 | `operating_systems` (36) | `os_overview` (23) · `os_kernel_structure` (13) | Real OS courses (08574*, 28020, 95604, 96007, B0832, 88324, RT-OS 85728/78810/B5665) carry `processes_threads`, `cpu_scheduling`, `file_systems`, `io_management`, `virtual_memory`. Literacy users (Informatics 57828/60348/78510, Basic IT Skills B9254, Computer Science 07276*) carry `productivity_software` and `computer_software_basics` instead. |
| 7 | `object_oriented_programming` (36) | `oop_classes_objects` (24) · `oop_inheritance_polymorphism` (12) | Intro Python courses (18 users have `programming_in_python`) use classes and objects only. OOP-centred courses (70219, B8554, 95648, 04138, 81672, 09032, 95627) also carry `object_oriented_software_design`, `programming_in_java` and `design_patterns`. |
| 8 | `programming_in_python` (39) | `python_language_intro` (24) · `python_as_tool` (15) | In 15 courses (Data Mining 40720, IoT 81683, RT Systems 78810/B5665, Software Engineering 95627, Digital Health B8018, …) Python is only the vehicle. Better: record the implementation language as a **course attribute**, not a topic (see §3). |
| 9 | `computer_networks` (28) | `networking_overview` (21) · `layered_network_architecture` (7) (absorbs `osi_model`) | Network courses (28024, 58423, 70226, 93315, 95604, 95644, 91161) have per-layer topics (`transport_layer_protocols`, `link_layer_protocols`, …). The other 21 are literacy courses that co-assign `internet_world_wide_web` (14) and `productivity_software` (12). |
| 10 | `memory_allocation` (19) | `dynamic_memory_allocation` (9): heap/stack, malloc/new · `os_memory_management` (10) | **Homonym**: the topic co-occurs with `programming_in_c` (9) in programming courses (00819*, 29227, 88145, 93034) and with `cpu_scheduling` (10) in OS courses. |
| 11 | `search_algorithms` (15) | `state_space_search` (9) · `searching_in_collections` (6) | **Homonym**: the AI courses (72938, 81940, 98931, 91248, 87469, …) mean A*/heuristic search; the ADS and intro courses (11929-B, B4913, B1703, 88145) mean linear/binary search. |
| 12 | `concurrent_computing` (21) | `concurrent_programming_models` (10): language-level threads/actors/futures · OS-level part → existing `process_synchronization` (11) | The OS courses carry `processes_threads` and `process_synchronization`. The PL/OOP courses (04138, 70219, 81672, 95648, 17628, 66870) are about language-level models. |
| 13 | `artificial_intelligence` (25) | `ai_literacy_overview` (18) · `ai_foundations` (7) | The AI courses (72938, 81940, 90147, 91248, 93669, 98931, B0069) carry `ai_problem_solving` and `search_algorithms`. The others are humanities, fashion, vehicular and basic-IT courses. |
| 14 | `internet_world_wide_web` (25) | `internet_web_literacy` (19) · `web_architecture_fundamentals` (6) | Web technology courses (28659, 41731, 75835, 95605) co-assign `http_web_protocols` and `markup_languages`. The others are literacy courses. |
| 15 | `programming_basics` (26) | `program_concepts_overview` (13) · rest merged into `imperative_constructs` | The description ("Basic principles of programming") is empty of content and co-occurs with `imperative_constructs` in 14 courses. |
| 16 | `cloud_computing` (19) | `cloud_fundamentals` (14) (absorbs `cloud_service_models`) · `cloud_big_data_platforms` (5) | The description is biased toward "for Big Data", but most users are SE/distributed/virtualization courses (95627, B0010, 97431, 96642, 95646). |
| 17 | `software_testing` (19) | `unit_testing_basics` (7) · `software_testing_techniques` (12) | Programming labs (85285, B0063, B0064, 27311-A) vs SE courses that carry `software_lifecycle`, `continuous_integration` and `software_quality_assurance`. |
| 18 | `statistical_data_analysis` (19) | `descriptive_statistics` (19) (absorbs `descriptive_analytics`) · `inferential_statistics` (7) (absorbs `statistical_inference`) | The description mixes descriptive analysis and statistical tests. These are separate modules in any statistics curriculum. |
| 19 | `requirements_analysis` (17) | `software_requirements_engineering` (11) · `database_requirements_analysis` (6) | Six users are database courses (10906*, 70155, 95611, 28027, 28652) that co-assign `relational_database_design`. |
| 20 | `distributed_systems` (22) | `distributed_systems_fundamentals` (12) · `distributed_systems_overview` (10) | Peripheral users: OS 08574, Networks 28024, SysAdmin 88324/B0832, Blockchain 90748, MAS 91267, Vehicular 90074/96994. |
| 21 | `software_architecture` (19) | `architectural_styles` (13) · `architecture_design_documentation` (6) | Web/distributed courses use styles (client-server, microservices). SE courses (28021, 72939, 95627, B0010, B0009) cover the design process. |
| 22 | `data_visualization` (18) | `charting_basics` (14) · `visual_analytics_dashboards` (4) | BI/analytics courses (96142, 98671, 96143, 77933) vs basic plotting in programming and IT-skills courses. |
| 23 | `productivity_software` (17) | `word_processing_presentations` · `spreadsheets` (absorbs `spreadsheet_data_processing`, `spreadsheet_automation`) | These are distinct ECDL-like modules. Course lists are not separable from the evidence, so both are assigned to all 17 users. |

Also worth a look, not included in the YAML:
- `databases_sql` (17) and `database_basics` (13) overlap in 7 courses. Rename `databases_sql` to `sql_query_language` and keep `database_basics` for literacy courses.
- `large_language_models` (16) and `language_models` (11) overlap in 8 courses. This is handled as a merge into `transformer_architectures`, below.
- `data_models` (14) mixes DB data models with multimedia data.
- `markup_languages` (20) covers both HTML/CSS authoring and XML. It could be split into `html_css` and `xml_markup` (`xml_processing` already exists).

---

## 2. Merge candidates

95 groups in the YAML. They are grouped by kind below; `from` keys include the target when the target already exists.

### 2.1 Same concept, language-specific or differently named (highest impact)

| Into | From | Note |
|---|---|---|
| `functions` | functions (18), functions_recursion (16), programming_functions (2), r_functions (1) | Python vs C vs C++ vs R versions of one concept. The new description must be language-neutral. |
| `basic_data_types` | + r_data_types | Also strip "Python" from the description; 2 of the 23 users are C courses. |
| `scope_visibility` | + variable_scope | Same issue (Python vs C++). |
| `file_input_output` | file_handling (18), program_input_output (9) | Co-occur in 5 courses; same content. |
| `computer_organization_basics` | computer_operation (16), von_neumann_architecture (10), program_execution_model (10) | Heavy mutual co-occurrence (12 / 8 / 5 courses); all describe "how a computer executes a program". |
| `binary_information_representation` | binary_encoding (15), information_representation (6) | |
| `number_representation_arithmetic` | numeric_information_representation (7), binary_arithmetic (5) | |
| `instruction_set_machine_language` | instruction_set_architecture (8), machine_languages (8) | Co-occur in 5 of 8 courses. |
| `model_evaluation_selection` | model_evaluation (10), model_selection_validation (4), model_validation (2) | Embedding cosine 0.86–0.90. |
| `relational_database_design` | + database_design | cos 0.89 |
| `large_language_models` | + generative_ai_models | |
| `transformer_architectures` | language_models (11), attention_mechanisms (4) | `language_models` is described as "transformers and efficient attention"; `attention_mechanisms` is the CV-side duplicate. |
| `ml_fundamentals` | supervised_unsupervised_learning (16) | Pure umbrella; the children already exist. |
| `neural_network_fundamentals` | feedforward_neural_networks, neural_network_training | |
| `algorithmic_problem_solving` | computational_thinking (7) | |
| `language_syntax` | + grammar_definitions | Both are BNF. |
| `parallel_algorithms_models` | parallel_algorithms, parallel_computation_models | cos 0.96, 2 of 2 co-occur |
| `software_licensing`, `prototyping`, `data_lakes`, `microservices_architecture`, `object_detection`, `web_server_side_programming`, `usability_evaluation`, `big_data_management` | software_license, software_prototyping, data_lakehouse, microservice_patterns, object_recognition, server_side_application_development, usability_testing, big_data_platforms | Trivial synonym or variant pairs (cos 0.80–0.92). |

### 2.2 Topics that always co-occur and are one teaching unit

`digital_signatures_certificates` (4/4 co-occurrence) · `network_security` ← secure_network_communications + network_firewalls (5/5) · `data_governance_strategy` (3/3) · `distributed_replication_consistency` (3/3) · `knowledge_based_systems` ← production_rule_systems (3/3) · `effectful_programming` ← monadic_programming + monad_transformers · `cryptography` ← cryptographic_security_properties (4/4) + perfect_security · `network_configuration` ← network_configuration_protocols + network_configuration_management · `ux_interaction_design` ← user_experience_design + interaction_design (5/6) · `data_warehousing_bi` · `ci_cd` · `container_technologies` · `identity_federation_sso` · `reinforcement_learning` ← markov_decision_processes · `recursive_algorithms` ← divide_and_conquer_algorithms (the description already covers D&C) · `information_theory` ← data_compression · `computing_history` ← microprocessor_evolution + language_evolution.

### 2.3 Over-fragmented single-course families, collapsed to module-sized units

| Family (course) | Before → after | Into |
|---|---|---|
| Digital forensics (81676) | 18 → 4 | `digital_forensics_process`, `digital_forensics_analysis`, `forensic_reporting_testimony`, `forensic_evidence_sources` |
| Localization (96725) | 11 → 2 | `software_localization`, `localization_quality_assurance` |
| Quantum (B3567) | 11 → 3 | `quantum_computing_foundations`, `quantum_circuits_algorithms`, `quantum_and_post_quantum_cryptography` |
| Open Science (90157) | 8 → 1 | `open_science` (keep `fair_data_management` separate; it is shared by 3 courses) |
| Translation tech (B8536, B8603, 87898, …) | 13 → 4 | `computer_assisted_translation`, `machine_translation`, `mt_pre_post_editing`, `terminology_management` |
| CS education (90749) | 8 → 2 | `cs_education_foundations`, `cs_teaching_methods` |
| Virtualization (Virtual Systems courses) | 9 → 2 | `system_virtualization`, `network_virtualization` |
| Sensors (B5722) | 4 → 1 | `sensor_characterization` |
| XR / immersive | 5 → 1 | `extended_reality_fundamentals` (keep `immersive_hardware`, `immersive_interaction` and `vr_software_platforms` for the advanced lab) |
| Digital health (B8018) / health IS | 7 → 2 | `digital_health`, `healthcare_information_systems` |
| Type systems (81672) | 4 → 1 | `type_systems` |
| Other small groups | — | `system_hardening`, `electronic_document_management`, `digital_transformation`, `exhaustive_search_algorithms`, `backtracking_branch_and_bound`, `heuristic_local_search`, `parallel_architectures` ← hpc_architectures, `iot_systems` ← platforms/programming models, `mobile_application_development` ← cross-platform/app types, `asynchronous_reactive_programming`, `web_services` ← soap_wsdl, `remote_procedure_call` ← grpc, `relational_query_languages`, `ai_scheduling`, `ai_assisted_programming`, `text_vector_space_models`, `basic_text_processing` ← text_tokenization, `data_analytics_overview`, `data_serialization`, `message_oriented_middleware` ← enterprise_messaging, `resolution_unification`, `project_management` ← ict_project_management, `ontology_engineering` ← design patterns/methods, `it_security_basics` ← malicious_software |

Considered and **not** merged:
- `it_security` vs `digital_privacy_security`: different focus.
- `javascript_programming` vs `web_client_side_programming`: language vs platform.
- `ai_ethics` vs `ai_epistemology`.
- `memory_devices` vs `memory_hierarchy`: intro vs advanced level, which is a levels issue, not a synonym.
- The CV pipeline topics that always co-occur (`camera_calibration`, `image_filtering`, …): distinct concepts that happen to be taught together.
- `search_engines` vs `information_retrieval`: user-level vs technical.

---

## 3. Other issues

1. **Not really topics** (they belong to course attributes or meta-data, not the ontology):
   - Tooling/environment: `development_environment` (15), `jupyter_notebooks` (2), `python_as_tool` (proposed above), and the language tags `programming_in_*` / `*_programming` when the language is only the vehicle.
   - Research or learning methods: `systematic_literature_review` (3), `study_designs`, `data_collection_analysis` ("in a laboratory activity").
   - Course-logistics leaks: `linux_introduction` ("brief introduction … at the beginning of the course"), `web_page_processing` ("as identified in the course learning outcomes"), `hub_and_spoke_systems` ("as covered in the course").
2. **Course context leaking into global descriptions.** About 20 descriptions mention "the course" (`augmented_reality`, `parallel_architectures`, `microprocessor_evolution`, `syllogistic_logic`, `unification_resolution`, `digital_resource_design`, …). Others are biased to the first course that created them: `cloud_computing` ("for Big Data"), `basic_data_types` / `functions` / `scope_visibility` / `object_mutability` ("Python"), `markup_languages` ("Web-oriented digital resources"), `tcp_ip_protocols`. The prompt should require course-independent descriptions, and descriptions should be rewritten when a topic is reused.
3. **Inconsistent granularity by course.** Topics per course range from 2 to 36. Coarse outliers: 72796 (4 umbrella topics), 30376 Business Intelligence (2), B4940 (2), 73435 Project Management (3). Ultra-fine outliers: 81676 (19 topics, 17 singletons), 91258-B (27, 12 singletons), B8018 (36, 14 singletons), B0009 (29, 11 singletons). A target band of about 8–20 topics per course, enforced in the prompt or in post-processing, would help.
4. **Umbrella and children co-assigned.** This happens across families: ML (`machine_learning` + `supervised_unsupervised_learning` + specific algorithms), OS, networks, NN (`neural_networks_deep_learning` + CNN/RNN: 14 of 16 CNN courses also carry the umbrella), security (`it_security` + specific threats). Add a rule: assign either the umbrella *or* its children.
5. **Naming conventions.**
   - Language topics mix `programming_in_python` / `programming_in_c` / `programming_in_java` with `kotlin_programming` / `scala_programming` / `javascript_programming` / `csharp_dotnet_programming`. Normalize them to one pattern, ideally as attributes.
   - Level markers are inconsistent: `*_basics`, `*_fundamentals`, `*_overview`, "Introduction to …" in descriptions. Pick one level vocabulary (for example `_overview` = literacy, unmarked = core, `advanced_*`).
6. **Duplicate courses inflate counts.** Ten course pairs have *identical* topic sets (81610/88202, 97431/B5796, 87901/B6082, 95782/90730, B0074/B1459, 28012/91602, B5293/B8929, 11933/41169, 91264/90720, 95611/10906). 20 more topics are "shared" only by variants of the same course (same base ID or same name), so they are effectively singletons. Usage counts for module building should deduplicate these.
7. **Unused topics:** `distributed_consensus` and `mixture_of_experts` are assigned to no course. Drop them.
8. **Remaining singletons (~330 after the changes).** Most are legitimate specialist content: CV pipeline steps, real-time scheduling details, bioinformatics algorithms (`burrows_wheeler_transform`, `motif_finding`, `sequence_assembly`), healthcare-management items (`healthcare_payer_models`, `clinical_efficiency`). For module partitioning, attach them to a parent module rather than delete them. The YAML does not attempt that; a cheap next step is to nearest-neighbour each singleton to a topic used by ≥ 3 courses (embedding similarity) and review.

## Method

- Usage counts and co-occurrence come from the 293 per-course files.
- Embeddings: `paraphrase-multilingual-mpnet-base-v2` on "key words: description". Pairs with cosine ≥ 0.80 (133 pairs) were reviewed. Pairs between 0.70 and 0.80 with low co-occurrence (274 pairs) were also reviewed, because synonyms tend *not* to co-occur.
- Groups of topics with identical course sets were listed to find units that are always taught together.
- Effects were simulated by applying the YAML below to the per-course sets.

## Machine-applicable changes

Semantics:
- Apply `splits` first. Each split removes `topic` and gives each listed course the listed sub-topic keys. Course lists are **best-effort**, inferred from course names and co-assigned topics, and should be reviewed. Where a list was not separable, every user of the topic is assigned.
- Then apply `merges`. Each merge replaces every key in `from` with `into`, deduplicates per course, and sets the description.
- An `into` key may be newly created by a split (for example `ml_fundamentals`, `computer_organization_basics`) or may be an existing key.
- `drop_unused` lists topics with no course.

```yaml
apply_order: splits first, then merges (merge `into` keys may be keys created by a split or existing keys)
drop_unused: [distributed_consensus, mixture_of_experts]
merges:
- into: functions
  description: 'Functions/procedures: definition, parameter passing, return values; recursive functions (language-neutral).'
  from: [functions, functions_recursion, programming_functions, r_functions]
- into: basic_data_types
  description: Primitive/basic data types of a programming language and their operations (language-neutral).
  from: [basic_data_types, r_data_types]
- into: scope_visibility
  description: Scope and visibility rules for names and variables.
  from: [scope_visibility, variable_scope]
- into: file_input_output
  description: Console and file input/output in programs.
  from: [file_handling, program_input_output]
- into: computer_organization_basics
  description: Von Neumann model, CPU/memory/I/O at overview level, fetch-execute cycle.
  from: [computer_operation, von_neumann_architecture, program_execution_model]
- into: binary_information_representation
  description: Bits, bytes and binary encoding of information in digital systems.
  from: [binary_encoding, information_representation]
- into: number_representation_arithmetic
  description: Positional number systems, binary arithmetic, two's complement and floating point.
  from: [numeric_information_representation, binary_arithmetic]
- into: computer_software_basics
  description: Classes and levels of software (system vs application) and basic application programs.
  from: [computer_software_basics, software_levels]
- into: large_language_models
  description: 'Generative LLMs and generative-AI tools: capabilities, usage and limits.'
  from: [large_language_models, generative_ai_models]
- into: transformer_architectures
  description: Attention mechanisms and transformer architectures for language and vision.
  from: [language_models, attention_mechanisms]
- into: model_evaluation_selection
  description: Evaluation metrics, validation schemes and model selection for ML/data-mining models.
  from: [model_evaluation, model_selection_validation, model_validation]
- into: relational_database_design
  description: Conceptual (ER) and logical relational design, normalization.
  from: [relational_database_design, database_design]
- into: software_licensing
  description: Software licenses, copyright/copyleft, free vs proprietary software.
  from: [software_license, software_licensing]
- into: prototyping
  description: Low/high-fidelity prototyping of interactive and software systems.
  from: [prototyping, software_prototyping]
- into: digital_signatures_certificates
  description: Digital signatures, certificates and PKI trust.
  from: [digital_signatures, digital_certificates]
- into: language_syntax
  description: 'Formal syntax description: grammars, BNF/EBNF.'
  from: [language_syntax, grammar_definitions]
- into: parallel_algorithms_models
  description: Models of parallel computation (PRAM etc.) and parallel algorithm design.
  from: [parallel_algorithms, parallel_computation_models]
- into: system_virtualization
  description: Virtual machines, hypervisors and virtualized devices/file systems.
  from: [virtual_machines, virtual_systems, virtual_devices, virtual_file_systems]
- into: network_virtualization
  description: 'Virtual networks: VLAN/MPLS, virtual stacks, NFV and SDN-adjacent virtualization.'
  from: [virtual_networks, virtual_networking_stacks, virtual_channels_networking, distributed_network_virtualization, network_function_virtualization]
- into: digital_forensics_process
  description: 'Forensic investigation process: scene processing, acquisition, preservation, chain of custody.'
  from: [digital_forensics_investigations, forensic_scene_processing, forensic_data_acquisition, forensic_data_preservation, chain_of_custody]
- into: digital_forensics_analysis
  description: Analysis of digital evidence, forensic tools and validation of results.
  from: [digital_forensics_analysis, digital_forensics_tools, digital_forensics_validation]
- into: forensic_reporting_testimony
  description: Forensic reports, expert testimony and professional ethics.
  from: [forensic_report_writing, digital_expert_testimony, forensic_ethics]
- into: forensic_evidence_sources
  description: 'Source-specific forensics: mobile, network, cloud, VM, email/social media, file recovery, anti-forensics.'
  from: [mobile_device_forensics, network_forensics, cloud_forensics, virtual_machine_forensics, email_social_media_forensics, graphics_file_recovery, anti_forensics]
- into: software_localization
  description: 'Localization of software, web content and videogames: adaptation, placeholders, UI constraints, accessibility.'
  from: [software_localization, videogame_localization, web_content_localization, localization_translation_adaptation, localization_placeholders, localization_interface_constraints,
    localization_accessibility, localization_information_mining]
- into: localization_quality_assurance
  description: Editing, revision, bug prevention and quality assessment of localized products.
  from: [localization_editing_revision, localization_quality_assessment, localization_bug_prevention]
- into: quantum_computing_foundations
  description: Qubits, Hilbert spaces and the quantum computational model.
  from: [quantum_computing, qubits, complex_hilbert_spaces]
- into: quantum_circuits_algorithms
  description: Quantum circuits, algorithms (Deutsch, Grover, Shor), programming, graphical languages, error correction.
  from: [quantum_circuits, quantum_algorithms, quantum_programming, quantum_graphical_languages, quantum_error_correction]
- into: quantum_and_post_quantum_cryptography
  description: Quantum protocols/cryptography and post-quantum cryptography.
  from: [quantum_cryptography, quantum_protocols, post_quantum_cryptography]
- into: open_science
  description: 'Open Science principles and practices: open access, methods, metrics, peer review, infrastructures, reproducibility, open-source.'
  from: [open_science, open_access, open_methodology, open_metrics, open_peer_review, open_infrastructures, reproducibility, open_source_software]
- into: computer_assisted_translation
  description: 'CAT tools and workflows: translation memories, file formats, QA, AI features.'
  from: [computer_aided_translation, cat_tools, ai_translation_tools, translation_memories, translation_file_formats, translation_quality_assurance]
- into: machine_translation
  description: Machine translation engines and their evaluation.
  from: [machine_translation, machine_translation_evaluation]
- into: mt_pre_post_editing
  description: Pre-editing, prompting and post-editing around MT/GenAI translation.
  from: [translation_pre_editing, translation_post_editing, translation_prompting]
- into: terminology_management
  description: Terminology extraction and terminology databases.
  from: [terminology_management, terminology_extraction]
- into: cs_education_foundations
  description: 'CS education as a discipline: rationale, curricula, learning theories.'
  from: [computer_science_education, cs_curriculum_design, learning_theories]
- into: cs_teaching_methods
  description: 'CS didactics: teaching methods, making/unplugged, misconceptions, assessment.'
  from: [cs_teaching_methods, cs_education_pedagogy, making_based_computing_education, programming_learning_difficulties, educational_assessment]
- into: system_hardening
  description: OS and device hardening and OS security mechanisms.
  from: [operating_system_security_hardening, os_security, device_hardening]
- into: usability_evaluation
  description: 'Usability evaluation: heuristic evaluation, usability testing, user studies.'
  from: [usability_evaluation, usability_testing]
- into: ux_interaction_design
  description: User-centred interaction and experience design process.
  from: [user_experience_design, interaction_design]
- into: it_security_basics
  description: 'Everyday IT security: malware, phishing, safe practices.'
  from: [it_security, malicious_software]
- into: big_data_management
  description: Big Data challenges, platforms and storage/processing approaches.
  from: [big_data_management, big_data_platforms]
- into: data_lakes
  description: Data lake and lakehouse architectures.
  from: [data_lakes, data_lakehouse]
- into: data_governance_strategy
  description: Enterprise data strategy and data governance.
  from: [data_governance, enterprise_data_strategy]
- into: ai_scheduling
  description: Scheduling problems modeled and solved with AI/constraint techniques.
  from: [scheduling, constraint_scheduling]
- into: microservices_architecture
  description: Microservice architectural style and its design patterns.
  from: [microservices_architecture, microservice_patterns]
- into: container_technologies
  description: Containers (Docker) and container orchestration.
  from: [docker_containerization, container_orchestration]
- into: text_vector_space_models
  description: Vector-space text representations, TF-IDF, LSA.
  from: [text_vectorization, latent_semantic_analysis]
- into: basic_text_processing
  description: Tokenization and basic text normalization.
  from: [basic_text_processing, text_tokenization]
- into: data_analytics_overview
  description: Kinds of analytics (descriptive/diagnostic/predictive/prescriptive) and analytics processes.
  from: [data_analytics, analytics_types, prescriptive_analytics]
- into: web_server_side_programming
  description: Server-side web application development.
  from: [web_server_side_programming, server_side_application_development]
- into: data_serialization
  description: Serialization formats and (de)serialization of data/objects.
  from: [data_serialization, object_serialization]
- into: message_oriented_middleware
  description: Message-oriented and enterprise messaging middleware (JMS, ESB).
  from: [message_oriented_middleware, enterprise_messaging]
- into: object_detection
  description: Object detection/recognition in images and video.
  from: [object_detection, object_recognition]
- into: resolution_unification
  description: Unification and resolution in logic.
  from: [mathematical_logic, unification_resolution]
- into: electronic_document_management
  description: Electronic documents, dematerialization and document lifecycle/preservation.
  from: [electronic_document_management, electronic_documents, dematerialization, digital_document_lifecycle]
- into: ai_assisted_programming
  description: AI code generation and program synthesis with LLMs.
  from: [ai_code_generation, ai_program_synthesis]
- into: relational_query_languages
  description: Relational algebra and relational calculus.
  from: [relational_algebra, relational_calculus]
- into: digital_transformation
  description: Digital transformation concepts, industrial drivers and initiative design.
  from: [digital_transformation, industrial_digital_transformation, digital_transformation_design]
- into: exhaustive_search_algorithms
  description: Brute-force/exhaustive search.
  from: [exhaustive_search, brute_force_algorithms]
- into: backtracking_branch_and_bound
  description: Backtracking and branch-and-bound.
  from: [backtracking_algorithms, branch_and_bound]
- into: recursive_algorithms
  description: Recursion and divide-and-conquer.
  from: [recursive_algorithms, divide_and_conquer_algorithms]
- into: heuristic_local_search
  description: Heuristics and local-search methods.
  from: [heuristic_algorithms, local_search_algorithms]
- into: parallel_architectures
  description: 'Parallel/HPC architectures: Flynn taxonomy, shared vs distributed memory, GPGPU.'
  from: [parallel_architectures, hpc_architectures]
- into: iot_systems
  description: IoT system architecture, platforms and programming models.
  from: [iot_systems, iot_platforms, iot_programming_models]
- into: identity_federation_sso
  description: Identity management, SSO and federated authentication (Kerberos, LDAP, OAuth, SAML).
  from: [identity_management, single_sign_on, federated_authentication]
- into: network_security
  description: 'Network security: secure channels/VPNs, firewalls, IDS.'
  from: [network_security, secure_network_communications, network_firewalls]
- into: extended_reality_fundamentals
  description: AR/VR/MR concepts, continuum and platforms.
  from: [augmented_reality, virtual_reality, mixed_reality, immersive_technologies, immersive_platforms]
- into: sensor_characterization
  description: Sensor principles, models, measurements and noise.
  from: [sensor_fundamentals, sensor_modeling, sensor_measurements, sensor_noise]
- into: data_warehousing_bi
  description: Business intelligence and data warehousing, OLAP.
  from: [data_warehousing, business_intelligence]
- into: asynchronous_reactive_programming
  description: Futures/promises, async/await and reactive streams.
  from: [asynchronous_programming, reactive_programming]
- into: web_services
  description: Web services implementations (SOAP/WSDL).
  from: [web_services, soap_wsdl]
- into: remote_procedure_call
  description: RPC/RMI and gRPC.
  from: [remote_procedure_call, grpc]
- into: ci_cd
  description: Continuous integration and delivery/deployment.
  from: [continuous_integration, continuous_delivery]
- into: project_management
  description: Project management including ICT projects.
  from: [project_management, ict_project_management]
- into: type_systems
  description: Type checking, parametric/dependent types, type classes and traits.
  from: [type_checking, parametric_types, type_classes_traits, dependent_types]
- into: instruction_set_machine_language
  description: ISA and machine language.
  from: [instruction_set_architecture, machine_languages]
- into: computing_history
  description: History of computing, processors and programming languages.
  from: [computing_history, microprocessor_evolution, language_evolution]
- into: information_theory
  description: Information, entropy and data compression.
  from: [information_theory, data_compression]
- into: mobile_application_development
  description: Native, hybrid and cross-platform mobile app development.
  from: [mobile_application_development, cross_platform_mobile_frameworks, web_mobile_application_types]
- into: effectful_programming
  description: 'Programming with effects: monads and monad transformers.'
  from: [effectful_programming, monadic_programming, monad_transformers]
- into: distributed_replication_consistency
  description: Replication and consistency models in distributed systems.
  from: [distributed_replication, distributed_consistency]
- into: knowledge_based_systems
  description: Knowledge-based and production-rule systems.
  from: [knowledge_based_systems, production_rule_systems]
- into: healthcare_information_systems
  description: Hospital/laboratory information systems and health databases.
  from: [healthcare_information_systems, healthcare_data_management, medical_informatics_software]
- into: digital_health
  description: Digital/personal/mobile health systems.
  from: [digital_health, personal_health_informatics, personal_health_cybernetics, mobile_health]
- into: cryptography
  description: 'Cryptography: security properties, perfect secrecy, symmetric and public-key algorithms.'
  from: [cryptography, cryptographic_security_properties, perfect_security]
- into: network_configuration
  description: Network configuration protocols and interface/routing configuration.
  from: [network_configuration_protocols, network_configuration_management]
- into: reinforcement_learning
  description: Reinforcement learning and Markov decision processes.
  from: [reinforcement_learning, markov_decision_processes]
- into: ontology_engineering
  description: Ontology engineering, design patterns and methods.
  from: [ontology_engineering, ontology_design_patterns, ontological_methods]
- into: spreadsheets
  description: Spreadsheet use for calculation and data processing, incl. macros.
  from: [spreadsheet_data_processing, spreadsheet_automation]
- into: descriptive_statistics
  description: Descriptive statistics of datasets.
  from: [descriptive_analytics]
- into: inferential_statistics
  description: Sampling, estimation, confidence intervals and hypothesis tests.
  from: [statistical_inference]
- into: cloud_fundamentals
  description: Cloud service and deployment models.
  from: [cloud_service_models]
- into: algorithmic_problem_solving
  description: From problem to algorithm; computational thinking.
  from: [computational_thinking]
- into: layered_network_architecture
  description: Layered protocol architectures (OSI, TCP/IP).
  from: [osi_model]
- into: neural_network_fundamentals
  description: MLPs, backpropagation and training.
  from: [feedforward_neural_networks, neural_network_training]
- into: ml_fundamentals
  description: Core ML concepts incl. supervised vs unsupervised paradigms.
  from: [supervised_unsupervised_learning]
splits:
- topic: machine_learning
  into:
  - key: ml_fundamentals
    description: 'Core ML concepts: learning problem formulation, training/validation/test, generalization, overfitting, bias-variance trade-off.'
    courses: ['81610', '88202', '91250', 91250-B, '93319', '95631', 95631-A, B2125, '91258', B0385, '85189', '90477', '91266', '73302', '91254', '95965', '93469',
      B1873, B2133]
  - key: applied_ml_workflow
    description: Practical end-to-end ML pipelines with libraries (data preparation, training, evaluation, deployment) in an application domain.
    courses: ['98735', B8018, '95638', '81683', B5722, '96143', '72796', '91261', '84401', '91259']
  - key: ml_literacy_overview
    description: Non-specialist overview of what machine learning is, typical applications, limits and societal impact.
    courses: ['78778', '81917', '81940', '85285', '90147', '93379', 93470-A, '93653', '95639', B0069, B0074, B1459, B4826, B5793]
- topic: neural_networks_deep_learning
  into:
  - key: neural_network_fundamentals
    description: Artificial neurons, MLPs, activation/loss functions, backpropagation and gradient-based training.
    courses: ['91250', 91250-A, 91250-B, '93319', '81610', '88202', '91258', B0385, 91258-A, 91258-B, '85189', '90477', B1873, '91266', 95631-A, '40720', B2133, '73302',
      '69661']
  - key: deep_learning_overview
    description: Conceptual overview of deep learning, its main architectures and application areas, without training details.
    courses: ['72796', '78778', '81940', '90147', '91259', '93469', '95639', '96143', '99195', B0009, B0063, B0069, B8018]
- topic: data_structures
  into:
  - key: builtin_collections
    description: Language-provided compound types (arrays/lists, tuples, records/structs, dictionaries, sets) and their use in introductory programs.
    courses: [00819, 00819-A, 00819-B, 09730, 09730-A, '15305', '16870', 27311-A, '28004', '28040', '29227', '41588', '65094', '75969', '85189', '85285', '85445',
      '88145', '89988', '93034', 93034-A, '93333', '95602', '96346', '98735', B0064, B1703, B2696, B4826, B5780, B8105, B8159, B8905]
  - key: linear_data_structures
    description: Linked lists, stacks and queues as abstract data types and their implementations.
    courses: [00819, 00819-A, '11929', 11929-A, 11929-B, '16692', '37635', '65094', '85301', B4913, B5782, B8554]
  - key: tree_graph_data_structures
    description: Trees (binary search trees, balanced trees) and graph representations as data structures.
    courses: ['11929', 11929-A, 11929-B, '37635', '85301', B4913, B5782]
- topic: algorithm_design_analysis
  into:
  - key: algorithmic_problem_solving
    description: 'From problem to algorithm: stepwise refinement, pseudocode/flowcharts, simple algorithm examples (absorbs computational_thinking).'
    courses: [00819-A, 00819-B, 07276-A, 07276-B, 09730, 09730-A, '15305', '15920', '16692', '28004', '28040', '28623', '29227', '41588', '60348', '73273', '75969',
      '81917', '88145', '93034', 93034-A, '95602', '96346', 96346-B, B0063, B0064, B1703, B4826, B8105, B8554]
  - key: algorithm_design_techniques
    description: Algorithm design paradigms and analysis of correctness and cost for non-trivial problems.
    courses: ['11929', 11929-A, 11929-B, '37635', '85301', B4913, B5782, '11933', '41169', '58437']
- topic: computer_hardware_architecture
  into:
  - key: computer_organization_basics
    description: 'Von Neumann model: CPU, memory, I/O and bus at overview level; fetch-execute cycle (absorbs computer_operation, von_neumann_architecture, program_execution_model).'
    courses: ['07276', 07276-B, 07276-C, 08574, 09730, 09730-A, '15305', '28004', '28106', '29227', 29227-A, '57828', '60348', '69259', '70210', '77780', '78510',
      '82202', '88145', '88146', '91685', 93034-A, '93653', '95604', '96346', 96346-B, B4826, B9254]
  - key: processor_system_architecture
    description: 'Processor and system architecture in depth: datapath, control, ISA, memory hierarchy, buses and I/O subsystems.'
    courses: ['03716', '11925', '28011', '28012', 28012-A, '69430', '69731', '91602', '96007', '95602']
- topic: operating_systems
  into:
  - key: os_overview
    description: Role, services and kinds of operating systems from the user's point of view.
    courses: ['07276', 07276-B, 07276-C, '20741', '28004', '28024', '28623', '29227', 29227-A, 29227-B, '37262', '57828', '60348', '70210', '78510', '88145', '91685',
      '93034', 93034-A, '93363', 96346-B, B8159, B9254]
  - key: os_kernel_structure
    description: Kernel organization, protection, system-call interface and OS service internals.
    courses: [08574, 08574-A, 08574-B, '28020', '95604', '96007', B0832, '88324', '85728', '78810', B5665, '88155', B8562]
- topic: object_oriented_programming
  into:
  - key: oop_classes_objects
    description: Classes, objects, methods, encapsulation and constructors at introductory level.
    courses: [00819, 00819-A, 07276-A, 08574, '16870', 27311-A, '28004', '28040', '29227', 29227-A, 29227-B, '37262', '78810', '85285', '89988', '93034', 93034-A,
      '98735', B2696, B5665, B5727, B5780, B8105, B8905]
  - key: oop_inheritance_polymorphism
    description: Inheritance, polymorphism, dynamic binding, interfaces/abstract classes and generics.
    courses: [04138, 09032, '70219', '81672', '81942', '95627', '95648', B8554, '65094', '93363', '37635', 11929-A]
- topic: programming_in_python
  into:
  - key: python_language_intro
    description: 'Python taught as a programming language: syntax, built-in types, control flow, functions, modules.'
    courses: [07276-A, '16870', 27311-A, '28040', '37262', '41588', '65094', '85285', '89988', 93034-A, '93333', '93363', '95602', '96346', '98735', B0063, B1703,
      B4826, B5727, B8905, B5780, B5782, B8105, B0064]
  - key: python_as_tool
    description: Python used as the implementation vehicle of an applied course (better modeled as a course attribute than a topic).
    courses: [08574-B, '37085', '40720', '72796', '78810', '81683', '85189', '90477', '91161', '95627', 95631-A, '95638', B1873, B5665, B8018]
- topic: computer_networks
  into:
  - key: networking_overview
    description: 'Basic networking concepts for non-specialists: LAN/WAN, devices, addressing, the Internet at a glance.'
    courses: ['07276', 07276-C, '20741', '28106', '28623', '37085', '37262', '39176', '57828', '60348', '69259', '73273', '75969', '77780', '78510', '82202', 82202-A,
      '88324', B4826, B8159, B9254]
  - key: layered_network_architecture
    description: Layered protocol architectures (OSI, TCP/IP), encapsulation and per-layer responsibilities (absorbs osi_model).
    courses: ['28024', '58423', '70226', '93315', '95604', '95644', '91161']
- topic: internet_world_wide_web
  into:
  - key: internet_web_literacy
    description: 'Using the Internet and the Web: browsers, e-mail, search, online services, basic risks.'
    courses: ['07276', 07276-B, 07276-C, '27311', '28106', '39176', '57828', '60348', '73273', '75969', '78510', '81917', '82202', 82202-A, '90157', '90864', B4826,
      B5293, B8929]
  - key: web_architecture_fundamentals
    description: 'Web architecture: hypertext, URLs, client/server interaction, browsers and servers as technical components.'
    courses: ['28659', '41731', '58423', '70226', '75835', '95605']
- topic: artificial_intelligence
  into:
  - key: ai_literacy_overview
    description: History, main approaches, applications and societal implications of AI for non-specialists.
    courses: [07276-B, 07276-C, '81610', '81917', '88202', '90074', '90864', '93653', 96346-B, '96994', B0070, B0074, B1459, B4948, B5293, B8603, B8929, B9254]
  - key: ai_foundations
    description: Agents, problem formulation and the map of AI sub-fields as the opening module of an AI course.
    courses: ['72938', '81940', '90147', '91248', '93669', '98931', B0069]
- topic: memory_allocation
  into:
  - key: dynamic_memory_allocation
    description: 'Program-level memory: stack vs heap, dynamic allocation/deallocation, memory errors.'
    courses: [00819, 00819-A, 00819-B, 04138, '28623', '29227', 29227-B, '88145', '93034']
  - key: os_memory_management
    description: 'OS memory management: partitioning, paging, segmentation, allocation policies.'
    courses: [08574, 08574-A, 08574-B, '28020', '78810', '85728', '88155', '95604', '96007', B5665]
- topic: concurrent_computing
  into:
  - key: concurrent_programming_models
    description: 'Language-level concurrency: threads, shared state, actors, futures, concurrent abstractions.'
    courses: [04138, '17628', '66870', '70219', '81672', '81942', '95648', B5727, B8562, B8563]
  - key: process_synchronization
    description: '(existing key) OS-level concurrency: race conditions, mutual exclusion, semaphores/monitors, deadlock.'
    courses: [08574, 08574-A, 08574-B, '28020', '58348', '78810', '85728', '88155', '96007', '96859', B5665]
- topic: search_algorithms
  into:
  - key: state_space_search
    description: 'AI search: uninformed and informed (heuristic) search, A*, adversarial search.'
    courses: ['72938', '78778', '81940', '87469', '91248', '91411', '91762', '98931', B0069]
  - key: searching_in_collections
    description: Linear and binary search over arrays/collections.
    courses: [11929-B, '16692', '88145', B1703, B4913, B8105]
- topic: cloud_computing
  into:
  - key: cloud_fundamentals
    description: Cloud service (IaaS/PaaS/SaaS) and deployment models, elasticity, providers (absorbs cloud_service_models).
    courses: ['17628', '37085', '81683', '81917', '81942', '95602', '95627', '95639', '95646', '96642', '96995', '97431', B0010, B5796]
  - key: cloud_big_data_platforms
    description: 'Running Big Data workloads on cloud clusters: on-premises vs cloud, managed data services, cost.'
    courses: ['81932', '84531', '93653', '95630', '96142']
- topic: software_testing
  into:
  - key: unit_testing_basics
    description: Writing and running unit tests/assertions as part of introductory programming.
    courses: [27311-A, '60348', '85285', B0063, B0064, '66860', '81672']
  - key: software_testing_techniques
    description: Test levels, black/white-box techniques, coverage, test planning in software engineering.
    courses: [09032, '28021', '66858', '72939', '81612', '90106', '95627', '97431', B0009, B0011, B1874, B5796]
- topic: statistical_data_analysis
  into:
  - key: descriptive_statistics
    description: 'Descriptive statistics: distributions, central tendency, dispersion, frequency tables (absorbs descriptive_analytics).'
    courses: ['40720', '41588', '65094', '79194', '81683', '81917', 82202-A, '85285', '88016', '89988', '93469', '95602', 95631-A, '95638', '96143', '96793', B5722,
      B8018, B8105]
  - key: inferential_statistics
    description: Sampling, estimation, confidence intervals and hypothesis tests (absorbs statistical_inference).
    courses: ['88016', '93469', 95631-A, '40720', '96143', B8018, '95638']
- topic: requirements_analysis
  into:
  - key: software_requirements_engineering
    description: Elicitation, specification (use cases, user stories) and validation of software requirements.
    courses: [09032, '28021', '66858', '72939', '73025', '85446', '90106', '94442', B0010, B0061, B1874]
  - key: database_requirements_analysis
    description: Collecting and structuring data requirements as the first step of database design.
    courses: ['10906', 10906-A, '28027', '28652', '70155', '95611']
- topic: programming_basics
  into:
  - key: program_concepts_overview
    description: What programs, algorithms and programming languages are; translation and execution at a conceptual level.
    courses: [07276-B, '28106', '57828', '69259', '81917', 96346-B, B2696, B0064, '96346', 09730, 09730-A, '15305', '28004']
  - key: imperative_constructs
    description: (existing key) Variables, assignment, selection and iteration.
    courses: ['28623', 29227-A, '37262', '85285', '88145', '89988', '93034', 93034-A, '93333', '98735', B1703, B5780, B8905]
- topic: distributed_systems
  into:
  - key: distributed_systems_fundamentals
    description: Goals, system models, transparency, failure models and architectures of distributed systems.
    courses: ['17628', '37085', '66870', '73025', '87474', '93468', '84531', '72939', B0009, '96642', '81942', '78779']
  - key: distributed_systems_overview
    description: Brief overview of distributed systems as context in another subject.
    courses: [08574, '28024', '84401', '88324', '90074', '90748', '91267', '95627', '96994', B0832]
- topic: data_visualization
  into:
  - key: charting_basics
    description: Basic charts and plots of datasets with spreadsheets or plotting libraries.
    courses: ['65094', '81683', '82202', 82202-A, '88016', '93379', '93653', '95602', '98735', B0063, B1703, B8018, B8105, B9254]
  - key: visual_analytics_dashboards
    description: Dashboards, interactive visual analytics and reporting for BI/analytics.
    courses: ['96142', '98671', '96143', '77933']
- topic: software_architecture
  into:
  - key: architectural_styles
    description: 'Architectural styles and patterns: layered, client-server, event-driven, SOA, microservices.'
    courses: ['28659', '73025', '84401', '87474', '93468', '94442', '95605', '97431', B5474, B5796, B8019, '99195', '90864']
  - key: architecture_design_documentation
    description: 'Architecture design process: logical architecture, views, quality attributes, architectural decisions.'
    courses: ['28021', '72939', '81612', '95627', B0009, B0010]
- topic: productivity_software
  into:
  - key: word_processing_presentations
    description: Word processors and presentation software.
    courses: ['07276', 07276-B, 07276-C, '15920', '28106', '37262', '39176', '57828', '73273', '78510', '82202', 82202-A, '93363', 96346-B, B4826, B8396, B9254]
  - key: spreadsheets
    description: Spreadsheet use for calculation and data processing (absorbs spreadsheet_data_processing, spreadsheet_automation).
    courses: ['07276', 07276-B, 07276-C, '15920', '28106', '37262', '39176', '57828', '73273', '78510', '82202', 82202-A, '93363', 96346-B, B4826, B8396, B9254]
```
