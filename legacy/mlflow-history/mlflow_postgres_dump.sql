--
-- PostgreSQL database dump
--

\restrict 7C3dd763wHcLKdy1CFZjhQCxt36G5k9K0qAX7U1uUXAVXpkfmNgfzdjFAESVIpM

-- Dumped from database version 15.17
-- Dumped by pg_dump version 15.17

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: prevent_secrets_aad_mutation(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.prevent_secrets_aad_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF OLD.secret_id != NEW.secret_id OR OLD.secret_name != NEW.secret_name THEN
        RAISE EXCEPTION 'secret_id and secret_name are immutable (used as AAD in encryption)';
    END IF;
    RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: alembic_version; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.alembic_version (
    version_num character varying(32) NOT NULL
);


--
-- Name: assessments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.assessments (
    assessment_id character varying(50) NOT NULL,
    trace_id character varying(50) NOT NULL,
    name character varying(250) NOT NULL,
    assessment_type character varying(20) NOT NULL,
    value text NOT NULL,
    error text,
    created_timestamp bigint NOT NULL,
    last_updated_timestamp bigint NOT NULL,
    source_type character varying(50) NOT NULL,
    source_id character varying(250),
    run_id character varying(32),
    span_id character varying(50),
    rationale text,
    overrides character varying(50),
    valid boolean NOT NULL,
    assessment_metadata text
);


--
-- Name: budget_policies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.budget_policies (
    budget_policy_id character varying(36) NOT NULL,
    budget_unit character varying(32) NOT NULL,
    budget_amount double precision NOT NULL,
    duration_unit character varying(32) NOT NULL,
    duration_value integer NOT NULL,
    target_scope character varying(32) NOT NULL,
    budget_action character varying(32) NOT NULL,
    created_by character varying(255),
    created_at bigint NOT NULL,
    last_updated_by character varying(255),
    last_updated_at bigint NOT NULL,
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL
);


--
-- Name: datasets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.datasets (
    dataset_uuid character varying(36) NOT NULL,
    experiment_id integer NOT NULL,
    name character varying(500) NOT NULL,
    digest character varying(36) NOT NULL,
    dataset_source_type character varying(36) NOT NULL,
    dataset_source text NOT NULL,
    dataset_schema text,
    dataset_profile text
);


--
-- Name: endpoint_bindings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.endpoint_bindings (
    endpoint_id character varying(36) NOT NULL,
    resource_type character varying(50) NOT NULL,
    resource_id character varying(255) NOT NULL,
    created_at bigint NOT NULL,
    created_by character varying(255),
    last_updated_at bigint NOT NULL,
    last_updated_by character varying(255),
    display_name character varying(255)
);


--
-- Name: endpoint_model_mappings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.endpoint_model_mappings (
    mapping_id character varying(36) NOT NULL,
    endpoint_id character varying(36) NOT NULL,
    model_definition_id character varying(36) NOT NULL,
    weight double precision NOT NULL,
    created_by character varying(255),
    created_at bigint NOT NULL,
    linkage_type character varying(64) DEFAULT 'PRIMARY'::character varying NOT NULL,
    fallback_order integer
);


--
-- Name: endpoint_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.endpoint_tags (
    key character varying(250) NOT NULL,
    value character varying(5000),
    endpoint_id character varying(36) NOT NULL
);


--
-- Name: endpoints; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.endpoints (
    endpoint_id character varying(36) NOT NULL,
    name character varying(255),
    created_by character varying(255),
    created_at bigint NOT NULL,
    last_updated_by character varying(255),
    last_updated_at bigint NOT NULL,
    routing_strategy character varying(64),
    fallback_config_json text,
    experiment_id integer,
    usage_tracking boolean DEFAULT false NOT NULL,
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL
);


--
-- Name: entity_associations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.entity_associations (
    association_id character varying(36) NOT NULL,
    source_type character varying(36) NOT NULL,
    source_id character varying(36) NOT NULL,
    destination_type character varying(36) NOT NULL,
    destination_id character varying(36) NOT NULL,
    created_time bigint
);


--
-- Name: evaluation_dataset_records; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.evaluation_dataset_records (
    dataset_record_id character varying(36) NOT NULL,
    dataset_id character varying(36) NOT NULL,
    inputs json NOT NULL,
    expectations json,
    tags json,
    source json,
    source_id character varying(36),
    source_type character varying(255),
    created_time bigint,
    last_update_time bigint,
    created_by character varying(255),
    last_updated_by character varying(255),
    input_hash character varying(64) NOT NULL,
    outputs json
);


--
-- Name: evaluation_dataset_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.evaluation_dataset_tags (
    dataset_id character varying(36) NOT NULL,
    key character varying(255) NOT NULL,
    value character varying(5000)
);


--
-- Name: evaluation_datasets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.evaluation_datasets (
    dataset_id character varying(36) NOT NULL,
    name character varying(255) NOT NULL,
    schema text,
    profile text,
    digest character varying(64),
    created_time bigint,
    last_update_time bigint,
    created_by character varying(255),
    last_updated_by character varying(255),
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL
);


--
-- Name: experiment_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiment_tags (
    key character varying(250) NOT NULL,
    value character varying(5000),
    experiment_id integer NOT NULL
);


--
-- Name: experiments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiments (
    experiment_id integer NOT NULL,
    name character varying(256) NOT NULL,
    artifact_location character varying(256),
    lifecycle_stage character varying(32),
    creation_time bigint,
    last_update_time bigint,
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL,
    CONSTRAINT experiments_lifecycle_stage CHECK (((lifecycle_stage)::text = ANY ((ARRAY['active'::character varying, 'deleted'::character varying])::text[])))
);


--
-- Name: experiments_experiment_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.experiments_experiment_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: experiments_experiment_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.experiments_experiment_id_seq OWNED BY public.experiments.experiment_id;


--
-- Name: guardrail_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.guardrail_configs (
    endpoint_id character varying(36) NOT NULL,
    guardrail_id character varying(36) NOT NULL,
    execution_order integer,
    created_by character varying(255),
    created_at bigint NOT NULL,
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL
);


--
-- Name: guardrails; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.guardrails (
    guardrail_id character varying(36) NOT NULL,
    name character varying(255) NOT NULL,
    scorer_id character varying(36) NOT NULL,
    scorer_version integer NOT NULL,
    stage character varying(32) NOT NULL,
    action character varying(32) NOT NULL,
    action_endpoint_id character varying(36),
    created_by character varying(255),
    created_at bigint NOT NULL,
    last_updated_by character varying(255),
    last_updated_at bigint NOT NULL,
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL
);


--
-- Name: input_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.input_tags (
    input_uuid character varying(36) NOT NULL,
    name character varying(255) NOT NULL,
    value character varying(500) NOT NULL
);


--
-- Name: inputs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.inputs (
    input_uuid character varying(36) NOT NULL,
    source_type character varying(36) NOT NULL,
    source_id character varying(36) NOT NULL,
    destination_type character varying(36) NOT NULL,
    destination_id character varying(36) NOT NULL,
    step bigint DEFAULT '0'::bigint NOT NULL
);


--
-- Name: issues; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.issues (
    issue_id character varying(36) NOT NULL,
    experiment_id integer NOT NULL,
    name character varying(250) NOT NULL,
    description text NOT NULL,
    status character varying(50) NOT NULL,
    severity character varying(50),
    root_causes text,
    source_run_id character varying(32),
    categories text,
    created_timestamp bigint NOT NULL,
    last_updated_timestamp bigint NOT NULL,
    created_by character varying(255)
);


--
-- Name: jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.jobs (
    id character varying(36) NOT NULL,
    creation_time bigint NOT NULL,
    job_name character varying(500) NOT NULL,
    params text NOT NULL,
    timeout double precision,
    status integer NOT NULL,
    result text,
    retry_count integer NOT NULL,
    last_update_time bigint NOT NULL,
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL,
    status_details json
);


--
-- Name: latest_metrics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.latest_metrics (
    key character varying(250) NOT NULL,
    value double precision NOT NULL,
    "timestamp" bigint,
    step bigint NOT NULL,
    is_nan boolean NOT NULL,
    run_uuid character varying(32) NOT NULL
);


--
-- Name: logged_model_metrics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.logged_model_metrics (
    model_id character varying(36) NOT NULL,
    metric_name character varying(500) NOT NULL,
    metric_timestamp_ms bigint NOT NULL,
    metric_step bigint NOT NULL,
    metric_value double precision,
    experiment_id integer NOT NULL,
    run_id character varying(32) NOT NULL,
    dataset_uuid character varying(36),
    dataset_name character varying(500),
    dataset_digest character varying(36)
);


--
-- Name: logged_model_params; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.logged_model_params (
    model_id character varying(36) NOT NULL,
    experiment_id integer NOT NULL,
    param_key character varying(255) NOT NULL,
    param_value text NOT NULL
);


--
-- Name: logged_model_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.logged_model_tags (
    model_id character varying(36) NOT NULL,
    experiment_id integer NOT NULL,
    tag_key character varying(255) NOT NULL,
    tag_value text NOT NULL
);


--
-- Name: logged_models; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.logged_models (
    model_id character varying(36) NOT NULL,
    experiment_id integer NOT NULL,
    name character varying(500) NOT NULL,
    artifact_location character varying(1000) NOT NULL,
    creation_timestamp_ms bigint NOT NULL,
    last_updated_timestamp_ms bigint NOT NULL,
    status integer NOT NULL,
    lifecycle_stage character varying(32),
    model_type character varying(500),
    source_run_id character varying(32),
    status_message character varying(1000),
    CONSTRAINT logged_models_lifecycle_stage_check CHECK (((lifecycle_stage)::text = ANY ((ARRAY['active'::character varying, 'deleted'::character varying])::text[])))
);


--
-- Name: metrics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.metrics (
    key character varying(250) NOT NULL,
    value double precision NOT NULL,
    "timestamp" bigint NOT NULL,
    run_uuid character varying(32) NOT NULL,
    step bigint DEFAULT '0'::bigint NOT NULL,
    is_nan boolean DEFAULT false NOT NULL
);


--
-- Name: model_definitions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.model_definitions (
    model_definition_id character varying(36) NOT NULL,
    name character varying(255) NOT NULL,
    secret_id character varying(36),
    provider character varying(64) NOT NULL,
    model_name character varying(256) NOT NULL,
    created_by character varying(255),
    created_at bigint NOT NULL,
    last_updated_by character varying(255),
    last_updated_at bigint NOT NULL,
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL
);


--
-- Name: model_version_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.model_version_tags (
    key character varying(250) NOT NULL,
    value text,
    name character varying(256) NOT NULL,
    version integer NOT NULL,
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL
);


--
-- Name: model_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.model_versions (
    name character varying(256) NOT NULL,
    version integer NOT NULL,
    creation_time bigint,
    last_updated_time bigint,
    description character varying(5000),
    user_id character varying(256),
    current_stage character varying(20),
    source character varying(500),
    run_id character varying(32),
    status character varying(20),
    status_message character varying(500),
    run_link character varying(500),
    storage_location character varying(500),
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL
);


--
-- Name: online_scoring_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.online_scoring_configs (
    online_scoring_config_id character varying(36) NOT NULL,
    scorer_id character varying(36) NOT NULL,
    sample_rate double precision NOT NULL,
    experiment_id integer NOT NULL,
    filter_string text
);


--
-- Name: params; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.params (
    key character varying(250) NOT NULL,
    value character varying(8000) NOT NULL,
    run_uuid character varying(32) NOT NULL
);


--
-- Name: registered_model_aliases; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.registered_model_aliases (
    alias character varying(256) NOT NULL,
    version integer NOT NULL,
    name character varying(256) NOT NULL,
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL
);


--
-- Name: registered_model_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.registered_model_tags (
    key character varying(250) NOT NULL,
    value character varying(5000),
    name character varying(256) NOT NULL,
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL
);


--
-- Name: registered_models; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.registered_models (
    name character varying(256) NOT NULL,
    creation_time bigint,
    last_updated_time bigint,
    description character varying(5000),
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL
);


--
-- Name: runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.runs (
    run_uuid character varying(32) NOT NULL,
    name character varying(250),
    source_type character varying(20),
    source_name character varying(500),
    entry_point_name character varying(50),
    user_id character varying(256),
    status character varying(9),
    start_time bigint,
    end_time bigint,
    source_version character varying(50),
    lifecycle_stage character varying(20),
    artifact_uri character varying(200),
    experiment_id integer,
    deleted_time bigint,
    CONSTRAINT runs_lifecycle_stage CHECK (((lifecycle_stage)::text = ANY ((ARRAY['active'::character varying, 'deleted'::character varying])::text[]))),
    CONSTRAINT runs_status_check CHECK (((status)::text = ANY ((ARRAY['SCHEDULED'::character varying, 'FAILED'::character varying, 'FINISHED'::character varying, 'RUNNING'::character varying, 'KILLED'::character varying])::text[]))),
    CONSTRAINT source_type CHECK (((source_type)::text = ANY ((ARRAY['NOTEBOOK'::character varying, 'JOB'::character varying, 'LOCAL'::character varying, 'UNKNOWN'::character varying, 'PROJECT'::character varying])::text[])))
);


--
-- Name: scorer_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scorer_versions (
    scorer_id character varying(36) NOT NULL,
    scorer_version integer NOT NULL,
    serialized_scorer text NOT NULL,
    creation_time bigint
);


--
-- Name: scorers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scorers (
    experiment_id integer NOT NULL,
    scorer_name character varying(256) NOT NULL,
    scorer_id character varying(36) NOT NULL
);


--
-- Name: secrets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.secrets (
    secret_id character varying(36) NOT NULL,
    secret_name character varying(255) NOT NULL,
    encrypted_value bytea NOT NULL,
    wrapped_dek bytea NOT NULL,
    kek_version integer NOT NULL,
    masked_value character varying(500) NOT NULL,
    provider character varying(64),
    auth_config text,
    description text,
    created_by character varying(255),
    created_at bigint NOT NULL,
    last_updated_by character varying(255),
    last_updated_at bigint NOT NULL,
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL
);


--
-- Name: span_metrics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.span_metrics (
    trace_id character varying(50) NOT NULL,
    span_id character varying(50) NOT NULL,
    key character varying(250) NOT NULL,
    value double precision
);


--
-- Name: spans; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.spans (
    trace_id character varying(50) NOT NULL,
    experiment_id integer NOT NULL,
    span_id character varying(50) NOT NULL,
    parent_span_id character varying(50),
    name text,
    type character varying(500),
    status character varying(50) NOT NULL,
    start_time_unix_nano bigint NOT NULL,
    end_time_unix_nano bigint,
    duration_ns bigint GENERATED ALWAYS AS ((end_time_unix_nano - start_time_unix_nano)) STORED,
    content text NOT NULL,
    dimension_attributes json
);


--
-- Name: tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tags (
    key character varying(250) NOT NULL,
    value character varying(8000),
    run_uuid character varying(32) NOT NULL
);


--
-- Name: trace_info; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trace_info (
    request_id character varying(50) NOT NULL,
    experiment_id integer NOT NULL,
    timestamp_ms bigint NOT NULL,
    execution_time_ms bigint,
    status character varying(50) NOT NULL,
    client_request_id character varying(50),
    request_preview character varying(1000),
    response_preview character varying(1000),
    db_payload_generation integer DEFAULT 0 NOT NULL
);


--
-- Name: trace_metrics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trace_metrics (
    request_id character varying(50) NOT NULL,
    key character varying(250) NOT NULL,
    value double precision
);


--
-- Name: trace_request_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trace_request_metadata (
    key character varying(250) NOT NULL,
    value character varying(8000),
    request_id character varying(50) NOT NULL
);


--
-- Name: trace_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trace_tags (
    key character varying(250) NOT NULL,
    value character varying(8000),
    request_id character varying(50) NOT NULL
);


--
-- Name: webhook_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.webhook_events (
    webhook_id character varying(256) NOT NULL,
    entity character varying(50) NOT NULL,
    action character varying(50) NOT NULL
);


--
-- Name: webhooks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.webhooks (
    webhook_id character varying(256) NOT NULL,
    name character varying(256) NOT NULL,
    description character varying(1000),
    url character varying(500) NOT NULL,
    status character varying(20) DEFAULT 'ACTIVE'::character varying NOT NULL,
    secret character varying(1000),
    creation_timestamp bigint,
    last_updated_timestamp bigint,
    deleted_timestamp bigint,
    workspace character varying(63) DEFAULT 'default'::character varying NOT NULL
);


--
-- Name: workspaces; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workspaces (
    name character varying(63) NOT NULL,
    description text,
    default_artifact_root text,
    trace_archival_location text,
    trace_archival_retention character varying(32)
);


--
-- Name: experiments experiment_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiments ALTER COLUMN experiment_id SET DEFAULT nextval('public.experiments_experiment_id_seq'::regclass);


--
-- Data for Name: alembic_version; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.alembic_version (version_num) FROM stdin;
da6fb0208061
\.


--
-- Data for Name: assessments; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.assessments (assessment_id, trace_id, name, assessment_type, value, error, created_timestamp, last_updated_timestamp, source_type, source_id, run_id, span_id, rationale, overrides, valid, assessment_metadata) FROM stdin;
\.


--
-- Data for Name: budget_policies; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.budget_policies (budget_policy_id, budget_unit, budget_amount, duration_unit, duration_value, target_scope, budget_action, created_by, created_at, last_updated_by, last_updated_at, workspace) FROM stdin;
\.


--
-- Data for Name: datasets; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.datasets (dataset_uuid, experiment_id, name, digest, dataset_source_type, dataset_source, dataset_schema, dataset_profile) FROM stdin;
\.


--
-- Data for Name: endpoint_bindings; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.endpoint_bindings (endpoint_id, resource_type, resource_id, created_at, created_by, last_updated_at, last_updated_by, display_name) FROM stdin;
\.


--
-- Data for Name: endpoint_model_mappings; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.endpoint_model_mappings (mapping_id, endpoint_id, model_definition_id, weight, created_by, created_at, linkage_type, fallback_order) FROM stdin;
\.


--
-- Data for Name: endpoint_tags; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.endpoint_tags (key, value, endpoint_id) FROM stdin;
\.


--
-- Data for Name: endpoints; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.endpoints (endpoint_id, name, created_by, created_at, last_updated_by, last_updated_at, routing_strategy, fallback_config_json, experiment_id, usage_tracking, workspace) FROM stdin;
\.


--
-- Data for Name: entity_associations; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.entity_associations (association_id, source_type, source_id, destination_type, destination_id, created_time) FROM stdin;
\.


--
-- Data for Name: evaluation_dataset_records; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.evaluation_dataset_records (dataset_record_id, dataset_id, inputs, expectations, tags, source, source_id, source_type, created_time, last_update_time, created_by, last_updated_by, input_hash, outputs) FROM stdin;
\.


--
-- Data for Name: evaluation_dataset_tags; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.evaluation_dataset_tags (dataset_id, key, value) FROM stdin;
\.


--
-- Data for Name: evaluation_datasets; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.evaluation_datasets (dataset_id, name, schema, profile, digest, created_time, last_update_time, created_by, last_updated_by, workspace) FROM stdin;
\.


--
-- Data for Name: experiment_tags; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.experiment_tags (key, value, experiment_id) FROM stdin;
\.


--
-- Data for Name: experiments; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.experiments (experiment_id, name, artifact_location, lifecycle_stage, creation_time, last_update_time, workspace) FROM stdin;
0	Default	/mlflow/artifacts/0	active	1779973485355	1779973485355	default
1	icp-price-prediction	/mlflow/artifacts/1	active	1779975326739	1779975326739	default
\.


--
-- Data for Name: guardrail_configs; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.guardrail_configs (endpoint_id, guardrail_id, execution_order, created_by, created_at, workspace) FROM stdin;
\.


--
-- Data for Name: guardrails; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.guardrails (guardrail_id, name, scorer_id, scorer_version, stage, action, action_endpoint_id, created_by, created_at, last_updated_by, last_updated_at, workspace) FROM stdin;
\.


--
-- Data for Name: input_tags; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.input_tags (input_uuid, name, value) FROM stdin;
\.


--
-- Data for Name: inputs; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.inputs (input_uuid, source_type, source_id, destination_type, destination_id, step) FROM stdin;
\.


--
-- Data for Name: issues; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.issues (issue_id, experiment_id, name, description, status, severity, root_causes, source_run_id, categories, created_timestamp, last_updated_timestamp, created_by) FROM stdin;
\.


--
-- Data for Name: jobs; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.jobs (id, creation_time, job_name, params, timeout, status, result, retry_count, last_update_time, workspace, status_details) FROM stdin;
\.


--
-- Data for Name: latest_metrics; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.latest_metrics (key, value, "timestamp", step, is_nan, run_uuid) FROM stdin;
rmse	3.6163761878506335	1779975326843	0	f	8aff486e4fba4ce59b98bd4012ab001e
mae	1.915533657281837	1779975326855	0	f	8aff486e4fba4ce59b98bd4012ab001e
rmse	5.553617331673714	1779975330920	0	f	6c89e215927f4e07847823855ee728c4
mae	3.316266666666664	1779975330930	0	f	6c89e215927f4e07847823855ee728c4
rmse	4.9530508798786546	1779975333077	0	f	a50a76130861484c85bdcce269e6c0c7
mae	3.045893333333328	1779975333091	0	f	a50a76130861484c85bdcce269e6c0c7
rmse	4.89042877813388	1779975335293	0	f	4b58ae3339b1421da7f3e36d772421fb
mae	2.957926666666672	1779975335302	0	f	4b58ae3339b1421da7f3e36d772421fb
\.


--
-- Data for Name: logged_model_metrics; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.logged_model_metrics (model_id, metric_name, metric_timestamp_ms, metric_step, metric_value, experiment_id, run_id, dataset_uuid, dataset_name, dataset_digest) FROM stdin;
\.


--
-- Data for Name: logged_model_params; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.logged_model_params (model_id, experiment_id, param_key, param_value) FROM stdin;
\.


--
-- Data for Name: logged_model_tags; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.logged_model_tags (model_id, experiment_id, tag_key, tag_value) FROM stdin;
\.


--
-- Data for Name: logged_models; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.logged_models (model_id, experiment_id, name, artifact_location, creation_timestamp_ms, last_updated_timestamp_ms, status, lifecycle_stage, model_type, source_run_id, status_message) FROM stdin;
\.


--
-- Data for Name: metrics; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.metrics (key, value, "timestamp", run_uuid, step, is_nan) FROM stdin;
rmse	3.6163761878506335	1779975326843	8aff486e4fba4ce59b98bd4012ab001e	0	f
mae	1.915533657281837	1779975326855	8aff486e4fba4ce59b98bd4012ab001e	0	f
rmse	5.553617331673714	1779975330920	6c89e215927f4e07847823855ee728c4	0	f
mae	3.316266666666664	1779975330930	6c89e215927f4e07847823855ee728c4	0	f
rmse	4.9530508798786546	1779975333077	a50a76130861484c85bdcce269e6c0c7	0	f
mae	3.045893333333328	1779975333091	a50a76130861484c85bdcce269e6c0c7	0	f
rmse	4.89042877813388	1779975335293	4b58ae3339b1421da7f3e36d772421fb	0	f
mae	2.957926666666672	1779975335302	4b58ae3339b1421da7f3e36d772421fb	0	f
\.


--
-- Data for Name: model_definitions; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.model_definitions (model_definition_id, name, secret_id, provider, model_name, created_by, created_at, last_updated_by, last_updated_at, workspace) FROM stdin;
\.


--
-- Data for Name: model_version_tags; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.model_version_tags (key, value, name, version, workspace) FROM stdin;
candidate	true	ICP_Price_Model	1	default
\.


--
-- Data for Name: model_versions; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.model_versions (name, version, creation_time, last_updated_time, description, user_id, current_stage, source, run_id, status, status_message, run_link, storage_location, workspace) FROM stdin;
ICP_Price_Model	1	1779975339037	1779975340364		\N	Production	/mlflow/artifacts/1/8aff486e4fba4ce59b98bd4012ab001e/artifacts/model	8aff486e4fba4ce59b98bd4012ab001e	READY	\N		/mlflow/artifacts/1/8aff486e4fba4ce59b98bd4012ab001e/artifacts/model	default
\.


--
-- Data for Name: online_scoring_configs; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.online_scoring_configs (online_scoring_config_id, scorer_id, sample_rate, experiment_id, filter_string) FROM stdin;
\.


--
-- Data for Name: params; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.params (key, value, run_uuid) FROM stdin;
model_type	LinearRegression	8aff486e4fba4ce59b98bd4012ab001e
model_type	RandomForest_n10	6c89e215927f4e07847823855ee728c4
n_estimators	10	6c89e215927f4e07847823855ee728c4
model_type	RandomForest_n50	a50a76130861484c85bdcce269e6c0c7
n_estimators	50	a50a76130861484c85bdcce269e6c0c7
model_type	RandomForest_n100	4b58ae3339b1421da7f3e36d772421fb
n_estimators	100	4b58ae3339b1421da7f3e36d772421fb
\.


--
-- Data for Name: registered_model_aliases; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.registered_model_aliases (alias, version, name, workspace) FROM stdin;
\.


--
-- Data for Name: registered_model_tags; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.registered_model_tags (key, value, name, workspace) FROM stdin;
\.


--
-- Data for Name: registered_models; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.registered_models (name, creation_time, last_updated_time, description, workspace) FROM stdin;
ICP_Price_Model	1779975338999	1779975340364		default
\.


--
-- Data for Name: runs; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.runs (run_uuid, name, source_type, source_name, entry_point_name, user_id, status, start_time, end_time, source_version, lifecycle_stage, artifact_uri, experiment_id, deleted_time) FROM stdin;
8aff486e4fba4ce59b98bd4012ab001e	LinearRegression	UNKNOWN			root	FINISHED	1779975326808	1779975330785		active	/mlflow/artifacts/1/8aff486e4fba4ce59b98bd4012ab001e/artifacts	1	\N
6c89e215927f4e07847823855ee728c4	RandomForest_n10	UNKNOWN			root	FINISHED	1779975330879	1779975332975		active	/mlflow/artifacts/1/6c89e215927f4e07847823855ee728c4/artifacts	1	\N
a50a76130861484c85bdcce269e6c0c7	RandomForest_n50	UNKNOWN			root	FINISHED	1779975333000	1779975335168		active	/mlflow/artifacts/1/a50a76130861484c85bdcce269e6c0c7/artifacts	1	\N
4b58ae3339b1421da7f3e36d772421fb	RandomForest_n100	UNKNOWN			root	FINISHED	1779975335191	1779975337486		active	/mlflow/artifacts/1/4b58ae3339b1421da7f3e36d772421fb/artifacts	1	\N
\.


--
-- Data for Name: scorer_versions; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.scorer_versions (scorer_id, scorer_version, serialized_scorer, creation_time) FROM stdin;
\.


--
-- Data for Name: scorers; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.scorers (experiment_id, scorer_name, scorer_id) FROM stdin;
\.


--
-- Data for Name: secrets; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.secrets (secret_id, secret_name, encrypted_value, wrapped_dek, kek_version, masked_value, provider, auth_config, description, created_by, created_at, last_updated_by, last_updated_at, workspace) FROM stdin;
\.


--
-- Data for Name: span_metrics; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.span_metrics (trace_id, span_id, key, value) FROM stdin;
\.


--
-- Data for Name: spans; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.spans (trace_id, experiment_id, span_id, parent_span_id, name, type, status, start_time_unix_nano, end_time_unix_nano, content, dimension_attributes) FROM stdin;
\.


--
-- Data for Name: tags; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.tags (key, value, run_uuid) FROM stdin;
mlflow.user	root	8aff486e4fba4ce59b98bd4012ab001e
mlflow.source.name	/app/src/training/train.py	8aff486e4fba4ce59b98bd4012ab001e
mlflow.source.type	LOCAL	8aff486e4fba4ce59b98bd4012ab001e
mlflow.runName	LinearRegression	8aff486e4fba4ce59b98bd4012ab001e
mlflow.log-model.history	[{"run_id": "8aff486e4fba4ce59b98bd4012ab001e", "artifact_path": "model", "utc_time_created": "2026-05-28 13:35:26.864585", "model_uuid": "7d15ea520b5e48b1a3338a2f665465e7", "flavors": {"python_function": {"model_path": "model.pkl", "predict_fn": "predict", "loader_module": "mlflow.sklearn", "python_version": "3.10.20", "env": {"conda": "conda.yaml", "virtualenv": "python_env.yaml"}}, "sklearn": {"pickled_model": "model.pkl", "sklearn_version": "1.7.2", "serialization_format": "cloudpickle", "code": null}}}]	8aff486e4fba4ce59b98bd4012ab001e
mlflow.user	root	6c89e215927f4e07847823855ee728c4
mlflow.source.name	/app/src/training/train.py	6c89e215927f4e07847823855ee728c4
mlflow.source.type	LOCAL	6c89e215927f4e07847823855ee728c4
mlflow.runName	RandomForest_n10	6c89e215927f4e07847823855ee728c4
mlflow.log-model.history	[{"run_id": "6c89e215927f4e07847823855ee728c4", "artifact_path": "model", "utc_time_created": "2026-05-28 13:35:30.941289", "model_uuid": "3f8e2ddcbc6240009e58aa1ca25385f5", "flavors": {"python_function": {"model_path": "model.pkl", "predict_fn": "predict", "loader_module": "mlflow.sklearn", "python_version": "3.10.20", "env": {"conda": "conda.yaml", "virtualenv": "python_env.yaml"}}, "sklearn": {"pickled_model": "model.pkl", "sklearn_version": "1.7.2", "serialization_format": "cloudpickle", "code": null}}}]	6c89e215927f4e07847823855ee728c4
mlflow.user	root	a50a76130861484c85bdcce269e6c0c7
mlflow.source.name	/app/src/training/train.py	a50a76130861484c85bdcce269e6c0c7
mlflow.source.type	LOCAL	a50a76130861484c85bdcce269e6c0c7
mlflow.runName	RandomForest_n50	a50a76130861484c85bdcce269e6c0c7
mlflow.log-model.history	[{"run_id": "a50a76130861484c85bdcce269e6c0c7", "artifact_path": "model", "utc_time_created": "2026-05-28 13:35:33.103064", "model_uuid": "658bdbd67e3047348ed18ace32b327f6", "flavors": {"python_function": {"model_path": "model.pkl", "predict_fn": "predict", "loader_module": "mlflow.sklearn", "python_version": "3.10.20", "env": {"conda": "conda.yaml", "virtualenv": "python_env.yaml"}}, "sklearn": {"pickled_model": "model.pkl", "sklearn_version": "1.7.2", "serialization_format": "cloudpickle", "code": null}}}]	a50a76130861484c85bdcce269e6c0c7
mlflow.user	root	4b58ae3339b1421da7f3e36d772421fb
mlflow.source.name	/app/src/training/train.py	4b58ae3339b1421da7f3e36d772421fb
mlflow.source.type	LOCAL	4b58ae3339b1421da7f3e36d772421fb
mlflow.runName	RandomForest_n100	4b58ae3339b1421da7f3e36d772421fb
mlflow.log-model.history	[{"run_id": "4b58ae3339b1421da7f3e36d772421fb", "artifact_path": "model", "utc_time_created": "2026-05-28 13:35:35.310218", "model_uuid": "37dbac9b86f04f558f78fda508b4e777", "flavors": {"python_function": {"model_path": "model.pkl", "predict_fn": "predict", "loader_module": "mlflow.sklearn", "python_version": "3.10.20", "env": {"conda": "conda.yaml", "virtualenv": "python_env.yaml"}}, "sklearn": {"pickled_model": "model.pkl", "sklearn_version": "1.7.2", "serialization_format": "cloudpickle", "code": null}}}]	4b58ae3339b1421da7f3e36d772421fb
\.


--
-- Data for Name: trace_info; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.trace_info (request_id, experiment_id, timestamp_ms, execution_time_ms, status, client_request_id, request_preview, response_preview, db_payload_generation) FROM stdin;
\.


--
-- Data for Name: trace_metrics; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.trace_metrics (request_id, key, value) FROM stdin;
\.


--
-- Data for Name: trace_request_metadata; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.trace_request_metadata (key, value, request_id) FROM stdin;
\.


--
-- Data for Name: trace_tags; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.trace_tags (key, value, request_id) FROM stdin;
\.


--
-- Data for Name: webhook_events; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.webhook_events (webhook_id, entity, action) FROM stdin;
\.


--
-- Data for Name: webhooks; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.webhooks (webhook_id, name, description, url, status, secret, creation_timestamp, last_updated_timestamp, deleted_timestamp, workspace) FROM stdin;
\.


--
-- Data for Name: workspaces; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.workspaces (name, description, default_artifact_root, trace_archival_location, trace_archival_retention) FROM stdin;
default	Default workspace for legacy resources	\N	\N	\N
\.


--
-- Name: experiments_experiment_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.experiments_experiment_id_seq', 1, true);


--
-- Name: alembic_version alembic_version_pkc; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alembic_version
    ADD CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num);


--
-- Name: assessments assessments_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.assessments
    ADD CONSTRAINT assessments_pk PRIMARY KEY (assessment_id);


--
-- Name: budget_policies budget_policies_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.budget_policies
    ADD CONSTRAINT budget_policies_pk PRIMARY KEY (budget_policy_id);


--
-- Name: datasets dataset_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datasets
    ADD CONSTRAINT dataset_pk PRIMARY KEY (experiment_id, name, digest);


--
-- Name: endpoint_bindings endpoint_bindings_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.endpoint_bindings
    ADD CONSTRAINT endpoint_bindings_pk PRIMARY KEY (endpoint_id, resource_type, resource_id);


--
-- Name: endpoint_model_mappings endpoint_model_mappings_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.endpoint_model_mappings
    ADD CONSTRAINT endpoint_model_mappings_pk PRIMARY KEY (mapping_id);


--
-- Name: endpoint_tags endpoint_tag_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.endpoint_tags
    ADD CONSTRAINT endpoint_tag_pk PRIMARY KEY (key, endpoint_id);


--
-- Name: endpoints endpoints_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.endpoints
    ADD CONSTRAINT endpoints_pk PRIMARY KEY (endpoint_id);


--
-- Name: entity_associations entity_associations_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_associations
    ADD CONSTRAINT entity_associations_pk PRIMARY KEY (source_type, source_id, destination_type, destination_id);


--
-- Name: evaluation_dataset_records evaluation_dataset_records_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.evaluation_dataset_records
    ADD CONSTRAINT evaluation_dataset_records_pk PRIMARY KEY (dataset_record_id);


--
-- Name: evaluation_dataset_tags evaluation_dataset_tags_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.evaluation_dataset_tags
    ADD CONSTRAINT evaluation_dataset_tags_pk PRIMARY KEY (dataset_id, key);


--
-- Name: evaluation_datasets evaluation_datasets_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.evaluation_datasets
    ADD CONSTRAINT evaluation_datasets_pk PRIMARY KEY (dataset_id);


--
-- Name: experiments experiment_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiments
    ADD CONSTRAINT experiment_pk PRIMARY KEY (experiment_id);


--
-- Name: experiment_tags experiment_tag_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_tags
    ADD CONSTRAINT experiment_tag_pk PRIMARY KEY (key, experiment_id);


--
-- Name: guardrail_configs guardrail_configs_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.guardrail_configs
    ADD CONSTRAINT guardrail_configs_pk PRIMARY KEY (endpoint_id, guardrail_id);


--
-- Name: guardrails guardrails_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.guardrails
    ADD CONSTRAINT guardrails_pk PRIMARY KEY (guardrail_id);


--
-- Name: input_tags input_tags_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.input_tags
    ADD CONSTRAINT input_tags_pk PRIMARY KEY (input_uuid, name);


--
-- Name: inputs inputs_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inputs
    ADD CONSTRAINT inputs_pk PRIMARY KEY (source_type, source_id, destination_type, destination_id);


--
-- Name: issues issues_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.issues
    ADD CONSTRAINT issues_pk PRIMARY KEY (issue_id);


--
-- Name: jobs jobs_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_pk PRIMARY KEY (id);


--
-- Name: latest_metrics latest_metric_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.latest_metrics
    ADD CONSTRAINT latest_metric_pk PRIMARY KEY (key, run_uuid);


--
-- Name: logged_model_metrics logged_model_metrics_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logged_model_metrics
    ADD CONSTRAINT logged_model_metrics_pk PRIMARY KEY (model_id, metric_name, metric_timestamp_ms, metric_step, run_id);


--
-- Name: logged_model_params logged_model_params_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logged_model_params
    ADD CONSTRAINT logged_model_params_pk PRIMARY KEY (model_id, param_key);


--
-- Name: logged_model_tags logged_model_tags_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logged_model_tags
    ADD CONSTRAINT logged_model_tags_pk PRIMARY KEY (model_id, tag_key);


--
-- Name: logged_models logged_models_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logged_models
    ADD CONSTRAINT logged_models_pk PRIMARY KEY (model_id);


--
-- Name: metrics metric_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metrics
    ADD CONSTRAINT metric_pk PRIMARY KEY (key, "timestamp", step, run_uuid, value, is_nan);


--
-- Name: model_definitions model_definitions_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_definitions
    ADD CONSTRAINT model_definitions_pk PRIMARY KEY (model_definition_id);


--
-- Name: model_versions model_version_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_versions
    ADD CONSTRAINT model_version_pk PRIMARY KEY (workspace, name, version);


--
-- Name: model_version_tags model_version_tag_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_version_tags
    ADD CONSTRAINT model_version_tag_pk PRIMARY KEY (workspace, key, name, version);


--
-- Name: online_scoring_configs online_scoring_config_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.online_scoring_configs
    ADD CONSTRAINT online_scoring_config_pk PRIMARY KEY (online_scoring_config_id);


--
-- Name: params param_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.params
    ADD CONSTRAINT param_pk PRIMARY KEY (key, run_uuid);


--
-- Name: registered_model_aliases registered_model_alias_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registered_model_aliases
    ADD CONSTRAINT registered_model_alias_pk PRIMARY KEY (workspace, name, alias);


--
-- Name: registered_models registered_model_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registered_models
    ADD CONSTRAINT registered_model_pk PRIMARY KEY (workspace, name);


--
-- Name: registered_model_tags registered_model_tag_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registered_model_tags
    ADD CONSTRAINT registered_model_tag_pk PRIMARY KEY (workspace, key, name);


--
-- Name: runs run_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT run_pk PRIMARY KEY (run_uuid);


--
-- Name: scorers scorer_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scorers
    ADD CONSTRAINT scorer_pk PRIMARY KEY (scorer_id);


--
-- Name: scorer_versions scorer_version_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scorer_versions
    ADD CONSTRAINT scorer_version_pk PRIMARY KEY (scorer_id, scorer_version);


--
-- Name: secrets secrets_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.secrets
    ADD CONSTRAINT secrets_pk PRIMARY KEY (secret_id);


--
-- Name: span_metrics span_metrics_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.span_metrics
    ADD CONSTRAINT span_metrics_pk PRIMARY KEY (trace_id, span_id, key);


--
-- Name: spans spans_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.spans
    ADD CONSTRAINT spans_pk PRIMARY KEY (trace_id, span_id);


--
-- Name: tags tag_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tag_pk PRIMARY KEY (key, run_uuid);


--
-- Name: trace_info trace_info_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trace_info
    ADD CONSTRAINT trace_info_pk PRIMARY KEY (request_id);


--
-- Name: trace_metrics trace_metrics_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trace_metrics
    ADD CONSTRAINT trace_metrics_pk PRIMARY KEY (request_id, key);


--
-- Name: trace_request_metadata trace_request_metadata_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trace_request_metadata
    ADD CONSTRAINT trace_request_metadata_pk PRIMARY KEY (key, request_id);


--
-- Name: trace_tags trace_tag_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trace_tags
    ADD CONSTRAINT trace_tag_pk PRIMARY KEY (key, request_id);


--
-- Name: evaluation_dataset_records unique_dataset_input; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.evaluation_dataset_records
    ADD CONSTRAINT unique_dataset_input UNIQUE (dataset_id, input_hash);


--
-- Name: endpoints uq_endpoints_workspace_name; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.endpoints
    ADD CONSTRAINT uq_endpoints_workspace_name UNIQUE (workspace, name);


--
-- Name: experiments uq_experiments_workspace_name; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiments
    ADD CONSTRAINT uq_experiments_workspace_name UNIQUE (workspace, name);


--
-- Name: model_definitions uq_model_definitions_workspace_name; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_definitions
    ADD CONSTRAINT uq_model_definitions_workspace_name UNIQUE (workspace, name);


--
-- Name: secrets uq_secrets_workspace_secret_name; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.secrets
    ADD CONSTRAINT uq_secrets_workspace_secret_name UNIQUE (workspace, secret_name);


--
-- Name: webhook_events webhook_event_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webhook_events
    ADD CONSTRAINT webhook_event_pk PRIMARY KEY (webhook_id, entity, action);


--
-- Name: webhooks webhook_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webhooks
    ADD CONSTRAINT webhook_pk PRIMARY KEY (webhook_id);


--
-- Name: workspaces workspaces_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workspaces
    ADD CONSTRAINT workspaces_pk PRIMARY KEY (name);


--
-- Name: idx_budget_policies_workspace; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_budget_policies_workspace ON public.budget_policies USING btree (workspace);


--
-- Name: idx_endpoints_workspace; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_endpoints_workspace ON public.endpoints USING btree (workspace);


--
-- Name: idx_evaluation_datasets_workspace; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_evaluation_datasets_workspace ON public.evaluation_datasets USING btree (workspace);


--
-- Name: idx_experiments_workspace; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_experiments_workspace ON public.experiments USING btree (workspace);


--
-- Name: idx_experiments_workspace_creation_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_experiments_workspace_creation_time ON public.experiments USING btree (workspace, creation_time);


--
-- Name: idx_guardrail_configs_endpoint_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_guardrail_configs_endpoint_id ON public.guardrail_configs USING btree (endpoint_id);


--
-- Name: idx_guardrail_configs_guardrail_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_guardrail_configs_guardrail_id ON public.guardrail_configs USING btree (guardrail_id);


--
-- Name: idx_guardrails_scorer; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_guardrails_scorer ON public.guardrails USING btree (scorer_id, scorer_version);


--
-- Name: idx_guardrails_workspace; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_guardrails_workspace ON public.guardrails USING btree (workspace);


--
-- Name: idx_model_definitions_workspace; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_model_definitions_workspace ON public.model_definitions USING btree (workspace);


--
-- Name: idx_model_version_tags_workspace_name_version; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_model_version_tags_workspace_name_version ON public.model_version_tags USING btree (workspace, name, version);


--
-- Name: idx_registered_model_tags_workspace_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_registered_model_tags_workspace_name ON public.registered_model_tags USING btree (workspace, name);


--
-- Name: idx_registered_models_workspace; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_registered_models_workspace ON public.registered_models USING btree (workspace);


--
-- Name: idx_secrets_workspace; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_secrets_workspace ON public.secrets USING btree (workspace);


--
-- Name: idx_webhook_events_action; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_webhook_events_action ON public.webhook_events USING btree (action);


--
-- Name: idx_webhook_events_entity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_webhook_events_entity ON public.webhook_events USING btree (entity);


--
-- Name: idx_webhook_events_entity_action; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_webhook_events_entity_action ON public.webhook_events USING btree (entity, action);


--
-- Name: idx_webhooks_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_webhooks_name ON public.webhooks USING btree (name);


--
-- Name: idx_webhooks_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_webhooks_status ON public.webhooks USING btree (status);


--
-- Name: idx_webhooks_workspace; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_webhooks_workspace ON public.webhooks USING btree (workspace);


--
-- Name: index_assessments_assessment_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_assessments_assessment_type ON public.assessments USING btree (assessment_type);


--
-- Name: index_assessments_last_updated_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_assessments_last_updated_timestamp ON public.assessments USING btree (last_updated_timestamp);


--
-- Name: index_assessments_run_id_created_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_assessments_run_id_created_timestamp ON public.assessments USING btree (run_id, created_timestamp);


--
-- Name: index_assessments_trace_id_created_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_assessments_trace_id_created_timestamp ON public.assessments USING btree (trace_id, created_timestamp);


--
-- Name: index_datasets_dataset_uuid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_datasets_dataset_uuid ON public.datasets USING btree (dataset_uuid);


--
-- Name: index_datasets_experiment_id_dataset_source_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_datasets_experiment_id_dataset_source_type ON public.datasets USING btree (experiment_id, dataset_source_type);


--
-- Name: index_endpoint_model_mappings_endpoint_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_endpoint_model_mappings_endpoint_id ON public.endpoint_model_mappings USING btree (endpoint_id);


--
-- Name: index_endpoint_model_mappings_model_definition_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_endpoint_model_mappings_model_definition_id ON public.endpoint_model_mappings USING btree (model_definition_id);


--
-- Name: index_endpoint_tags_endpoint_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_endpoint_tags_endpoint_id ON public.endpoint_tags USING btree (endpoint_id);


--
-- Name: index_entity_associations_association_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_entity_associations_association_id ON public.entity_associations USING btree (association_id);


--
-- Name: index_entity_associations_reverse_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_entity_associations_reverse_lookup ON public.entity_associations USING btree (destination_type, destination_id, source_type, source_id);


--
-- Name: index_evaluation_dataset_records_dataset_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_evaluation_dataset_records_dataset_id ON public.evaluation_dataset_records USING btree (dataset_id);


--
-- Name: index_evaluation_dataset_tags_dataset_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_evaluation_dataset_tags_dataset_id ON public.evaluation_dataset_tags USING btree (dataset_id);


--
-- Name: index_evaluation_datasets_created_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_evaluation_datasets_created_time ON public.evaluation_datasets USING btree (created_time);


--
-- Name: index_evaluation_datasets_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_evaluation_datasets_name ON public.evaluation_datasets USING btree (name);


--
-- Name: index_inputs_destination_type_destination_id_source_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_inputs_destination_type_destination_id_source_type ON public.inputs USING btree (destination_type, destination_id, source_type);


--
-- Name: index_inputs_input_uuid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_inputs_input_uuid ON public.inputs USING btree (input_uuid);


--
-- Name: index_issues_experiment_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_issues_experiment_id ON public.issues USING btree (experiment_id);


--
-- Name: index_issues_source_run_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_issues_source_run_id ON public.issues USING btree (source_run_id);


--
-- Name: index_issues_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_issues_status ON public.issues USING btree (status);


--
-- Name: index_jobs_name_status_creation_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_jobs_name_status_creation_time ON public.jobs USING btree (job_name, workspace, status, creation_time);


--
-- Name: index_latest_metrics_run_uuid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_latest_metrics_run_uuid ON public.latest_metrics USING btree (run_uuid);


--
-- Name: index_logged_model_metrics_model_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_logged_model_metrics_model_id ON public.logged_model_metrics USING btree (model_id);


--
-- Name: index_metrics_run_uuid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_metrics_run_uuid ON public.metrics USING btree (run_uuid);


--
-- Name: index_metrics_run_uuid_key_step; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_metrics_run_uuid_key_step ON public.metrics USING btree (run_uuid, key, step);


--
-- Name: index_model_definitions_provider; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_model_definitions_provider ON public.model_definitions USING btree (provider);


--
-- Name: index_model_definitions_secret_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_model_definitions_secret_id ON public.model_definitions USING btree (secret_id);


--
-- Name: index_params_run_uuid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_params_run_uuid ON public.params USING btree (run_uuid);


--
-- Name: index_scorer_versions_scorer_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_scorer_versions_scorer_id ON public.scorer_versions USING btree (scorer_id);


--
-- Name: index_scorers_experiment_id_scorer_name; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX index_scorers_experiment_id_scorer_name ON public.scorers USING btree (experiment_id, scorer_name);


--
-- Name: index_span_metrics_trace_id_span_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_span_metrics_trace_id_span_id ON public.span_metrics USING btree (trace_id, span_id);


--
-- Name: index_spans_experiment_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_spans_experiment_id ON public.spans USING btree (experiment_id);


--
-- Name: index_spans_experiment_id_duration; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_spans_experiment_id_duration ON public.spans USING btree (experiment_id, duration_ns);


--
-- Name: index_spans_experiment_id_status_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_spans_experiment_id_status_type ON public.spans USING btree (experiment_id, status, type);


--
-- Name: index_spans_experiment_id_type_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_spans_experiment_id_type_status ON public.spans USING btree (experiment_id, type, status);


--
-- Name: index_tags_run_uuid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_tags_run_uuid ON public.tags USING btree (run_uuid);


--
-- Name: index_trace_info_experiment_id_timestamp_ms; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_trace_info_experiment_id_timestamp_ms ON public.trace_info USING btree (experiment_id, timestamp_ms);


--
-- Name: index_trace_metrics_request_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_trace_metrics_request_id ON public.trace_metrics USING btree (request_id);


--
-- Name: index_trace_request_metadata_request_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_trace_request_metadata_request_id ON public.trace_request_metadata USING btree (request_id);


--
-- Name: index_trace_tags_request_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_trace_tags_request_id ON public.trace_tags USING btree (request_id);


--
-- Name: unique_endpoint_model_linkage_mapping; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX unique_endpoint_model_linkage_mapping ON public.endpoint_model_mappings USING btree (endpoint_id, model_definition_id, linkage_type);


--
-- Name: secrets prevent_secrets_aad_mutation; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER prevent_secrets_aad_mutation BEFORE UPDATE ON public.secrets FOR EACH ROW EXECUTE FUNCTION public.prevent_secrets_aad_mutation();


--
-- Name: experiment_tags experiment_tags_experiment_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiment_tags
    ADD CONSTRAINT experiment_tags_experiment_id_fkey FOREIGN KEY (experiment_id) REFERENCES public.experiments(experiment_id);


--
-- Name: assessments fk_assessments_trace_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.assessments
    ADD CONSTRAINT fk_assessments_trace_id FOREIGN KEY (trace_id) REFERENCES public.trace_info(request_id) ON DELETE CASCADE;


--
-- Name: datasets fk_datasets_experiment_id_experiments; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datasets
    ADD CONSTRAINT fk_datasets_experiment_id_experiments FOREIGN KEY (experiment_id) REFERENCES public.experiments(experiment_id) ON DELETE CASCADE;


--
-- Name: endpoint_bindings fk_endpoint_bindings_endpoint_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.endpoint_bindings
    ADD CONSTRAINT fk_endpoint_bindings_endpoint_id FOREIGN KEY (endpoint_id) REFERENCES public.endpoints(endpoint_id) ON DELETE CASCADE;


--
-- Name: endpoint_model_mappings fk_endpoint_model_mappings_endpoint_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.endpoint_model_mappings
    ADD CONSTRAINT fk_endpoint_model_mappings_endpoint_id FOREIGN KEY (endpoint_id) REFERENCES public.endpoints(endpoint_id) ON DELETE CASCADE;


--
-- Name: endpoint_model_mappings fk_endpoint_model_mappings_model_definition_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.endpoint_model_mappings
    ADD CONSTRAINT fk_endpoint_model_mappings_model_definition_id FOREIGN KEY (model_definition_id) REFERENCES public.model_definitions(model_definition_id);


--
-- Name: endpoint_tags fk_endpoint_tags_endpoint_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.endpoint_tags
    ADD CONSTRAINT fk_endpoint_tags_endpoint_id FOREIGN KEY (endpoint_id) REFERENCES public.endpoints(endpoint_id) ON DELETE CASCADE;


--
-- Name: endpoints fk_endpoints_experiment_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.endpoints
    ADD CONSTRAINT fk_endpoints_experiment_id FOREIGN KEY (experiment_id) REFERENCES public.experiments(experiment_id) ON DELETE SET NULL;


--
-- Name: evaluation_dataset_records fk_evaluation_dataset_records_dataset_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.evaluation_dataset_records
    ADD CONSTRAINT fk_evaluation_dataset_records_dataset_id FOREIGN KEY (dataset_id) REFERENCES public.evaluation_datasets(dataset_id) ON DELETE CASCADE;


--
-- Name: evaluation_dataset_tags fk_evaluation_dataset_tags_dataset_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.evaluation_dataset_tags
    ADD CONSTRAINT fk_evaluation_dataset_tags_dataset_id FOREIGN KEY (dataset_id) REFERENCES public.evaluation_datasets(dataset_id) ON DELETE CASCADE;


--
-- Name: guardrail_configs fk_guardrail_configs_endpoint_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.guardrail_configs
    ADD CONSTRAINT fk_guardrail_configs_endpoint_id FOREIGN KEY (endpoint_id) REFERENCES public.endpoints(endpoint_id) ON DELETE CASCADE;


--
-- Name: guardrail_configs fk_guardrail_configs_guardrail_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.guardrail_configs
    ADD CONSTRAINT fk_guardrail_configs_guardrail_id FOREIGN KEY (guardrail_id) REFERENCES public.guardrails(guardrail_id) ON DELETE CASCADE;


--
-- Name: guardrails fk_guardrails_action_endpoint_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.guardrails
    ADD CONSTRAINT fk_guardrails_action_endpoint_id FOREIGN KEY (action_endpoint_id) REFERENCES public.endpoints(endpoint_id) ON DELETE SET NULL;


--
-- Name: guardrails fk_guardrails_scorer_version; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.guardrails
    ADD CONSTRAINT fk_guardrails_scorer_version FOREIGN KEY (scorer_id, scorer_version) REFERENCES public.scorer_versions(scorer_id, scorer_version);


--
-- Name: issues fk_issues_experiment_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.issues
    ADD CONSTRAINT fk_issues_experiment_id FOREIGN KEY (experiment_id) REFERENCES public.experiments(experiment_id) ON DELETE CASCADE;


--
-- Name: issues fk_issues_source_run_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.issues
    ADD CONSTRAINT fk_issues_source_run_id FOREIGN KEY (source_run_id) REFERENCES public.runs(run_uuid) ON DELETE SET NULL;


--
-- Name: logged_model_metrics fk_logged_model_metrics_experiment_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logged_model_metrics
    ADD CONSTRAINT fk_logged_model_metrics_experiment_id FOREIGN KEY (experiment_id) REFERENCES public.experiments(experiment_id);


--
-- Name: logged_model_metrics fk_logged_model_metrics_model_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logged_model_metrics
    ADD CONSTRAINT fk_logged_model_metrics_model_id FOREIGN KEY (model_id) REFERENCES public.logged_models(model_id) ON DELETE CASCADE;


--
-- Name: logged_model_metrics fk_logged_model_metrics_run_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logged_model_metrics
    ADD CONSTRAINT fk_logged_model_metrics_run_id FOREIGN KEY (run_id) REFERENCES public.runs(run_uuid) ON DELETE CASCADE;


--
-- Name: logged_model_params fk_logged_model_params_experiment_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logged_model_params
    ADD CONSTRAINT fk_logged_model_params_experiment_id FOREIGN KEY (experiment_id) REFERENCES public.experiments(experiment_id);


--
-- Name: logged_model_params fk_logged_model_params_model_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logged_model_params
    ADD CONSTRAINT fk_logged_model_params_model_id FOREIGN KEY (model_id) REFERENCES public.logged_models(model_id) ON DELETE CASCADE;


--
-- Name: logged_model_tags fk_logged_model_tags_experiment_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logged_model_tags
    ADD CONSTRAINT fk_logged_model_tags_experiment_id FOREIGN KEY (experiment_id) REFERENCES public.experiments(experiment_id);


--
-- Name: logged_model_tags fk_logged_model_tags_model_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logged_model_tags
    ADD CONSTRAINT fk_logged_model_tags_model_id FOREIGN KEY (model_id) REFERENCES public.logged_models(model_id) ON DELETE CASCADE;


--
-- Name: logged_models fk_logged_models_experiment_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.logged_models
    ADD CONSTRAINT fk_logged_models_experiment_id FOREIGN KEY (experiment_id) REFERENCES public.experiments(experiment_id) ON DELETE CASCADE;


--
-- Name: model_definitions fk_model_definitions_secret_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_definitions
    ADD CONSTRAINT fk_model_definitions_secret_id FOREIGN KEY (secret_id) REFERENCES public.secrets(secret_id) ON DELETE SET NULL;


--
-- Name: model_version_tags fk_model_version_tags_model_versions; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_version_tags
    ADD CONSTRAINT fk_model_version_tags_model_versions FOREIGN KEY (workspace, name, version) REFERENCES public.model_versions(workspace, name, version) ON UPDATE CASCADE;


--
-- Name: model_versions fk_model_versions_registered_models; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_versions
    ADD CONSTRAINT fk_model_versions_registered_models FOREIGN KEY (workspace, name) REFERENCES public.registered_models(workspace, name) ON UPDATE CASCADE;


--
-- Name: online_scoring_configs fk_online_scoring_configs_experiment_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.online_scoring_configs
    ADD CONSTRAINT fk_online_scoring_configs_experiment_id FOREIGN KEY (experiment_id) REFERENCES public.experiments(experiment_id);


--
-- Name: online_scoring_configs fk_online_scoring_configs_scorer_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.online_scoring_configs
    ADD CONSTRAINT fk_online_scoring_configs_scorer_id FOREIGN KEY (scorer_id) REFERENCES public.scorers(scorer_id) ON DELETE CASCADE;


--
-- Name: registered_model_aliases fk_registered_model_aliases_registered_models; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registered_model_aliases
    ADD CONSTRAINT fk_registered_model_aliases_registered_models FOREIGN KEY (workspace, name) REFERENCES public.registered_models(workspace, name) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: registered_model_tags fk_registered_model_tags_registered_models; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.registered_model_tags
    ADD CONSTRAINT fk_registered_model_tags_registered_models FOREIGN KEY (workspace, name) REFERENCES public.registered_models(workspace, name) ON UPDATE CASCADE;


--
-- Name: scorer_versions fk_scorer_versions_scorer_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scorer_versions
    ADD CONSTRAINT fk_scorer_versions_scorer_id FOREIGN KEY (scorer_id) REFERENCES public.scorers(scorer_id) ON DELETE CASCADE;


--
-- Name: scorers fk_scorers_experiment_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scorers
    ADD CONSTRAINT fk_scorers_experiment_id FOREIGN KEY (experiment_id) REFERENCES public.experiments(experiment_id) ON DELETE CASCADE;


--
-- Name: span_metrics fk_span_metrics_span; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.span_metrics
    ADD CONSTRAINT fk_span_metrics_span FOREIGN KEY (trace_id, span_id) REFERENCES public.spans(trace_id, span_id) ON DELETE CASCADE;


--
-- Name: spans fk_spans_experiment_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.spans
    ADD CONSTRAINT fk_spans_experiment_id FOREIGN KEY (experiment_id) REFERENCES public.experiments(experiment_id);


--
-- Name: spans fk_spans_trace_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.spans
    ADD CONSTRAINT fk_spans_trace_id FOREIGN KEY (trace_id) REFERENCES public.trace_info(request_id) ON DELETE CASCADE;


--
-- Name: trace_info fk_trace_info_experiment_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trace_info
    ADD CONSTRAINT fk_trace_info_experiment_id FOREIGN KEY (experiment_id) REFERENCES public.experiments(experiment_id);


--
-- Name: trace_metrics fk_trace_metrics_request_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trace_metrics
    ADD CONSTRAINT fk_trace_metrics_request_id FOREIGN KEY (request_id) REFERENCES public.trace_info(request_id) ON DELETE CASCADE;


--
-- Name: trace_request_metadata fk_trace_request_metadata_request_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trace_request_metadata
    ADD CONSTRAINT fk_trace_request_metadata_request_id FOREIGN KEY (request_id) REFERENCES public.trace_info(request_id) ON DELETE CASCADE;


--
-- Name: trace_tags fk_trace_tags_request_id; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trace_tags
    ADD CONSTRAINT fk_trace_tags_request_id FOREIGN KEY (request_id) REFERENCES public.trace_info(request_id) ON DELETE CASCADE;


--
-- Name: latest_metrics latest_metrics_run_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.latest_metrics
    ADD CONSTRAINT latest_metrics_run_uuid_fkey FOREIGN KEY (run_uuid) REFERENCES public.runs(run_uuid);


--
-- Name: metrics metrics_run_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metrics
    ADD CONSTRAINT metrics_run_uuid_fkey FOREIGN KEY (run_uuid) REFERENCES public.runs(run_uuid);


--
-- Name: params params_run_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.params
    ADD CONSTRAINT params_run_uuid_fkey FOREIGN KEY (run_uuid) REFERENCES public.runs(run_uuid);


--
-- Name: runs runs_experiment_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.runs
    ADD CONSTRAINT runs_experiment_id_fkey FOREIGN KEY (experiment_id) REFERENCES public.experiments(experiment_id);


--
-- Name: tags tags_run_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tags_run_uuid_fkey FOREIGN KEY (run_uuid) REFERENCES public.runs(run_uuid);


--
-- Name: webhook_events webhook_events_webhook_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webhook_events
    ADD CONSTRAINT webhook_events_webhook_id_fkey FOREIGN KEY (webhook_id) REFERENCES public.webhooks(webhook_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict 7C3dd763wHcLKdy1CFZjhQCxt36G5k9K0qAX7U1uUXAVXpkfmNgfzdjFAESVIpM

