
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- DROP TABLE public.hostname;

CREATE TABLE public.hostname (
	hostname_id uuid DEFAULT uuid_generate_v4() NOT NULL,
	hostname text NOT NULL,
	created_at timestamp NOT NULL,
	updated_at timestamp NOT NULL,
	updated_by text NOT NULL,
	CONSTRAINT hostname_hostname_key UNIQUE (hostname),
	CONSTRAINT hostname_pkey PRIMARY KEY (hostname_id)
);
CREATE INDEX hostname_index ON public.hostname USING btree (hostname);

-- DROP TABLE public.sessions;

CREATE TABLE public.sessions (
	session_id uuid DEFAULT uuid_generate_v4() NOT NULL,
	hostname_id uuid NOT NULL,
	host_name text NOT NULL,
	ip text NOT NULL,
	session_cookie text NOT NULL,
	ip_cookie text NOT NULL,
	primary_session int4 DEFAULT 0 NULL,
	human int4 DEFAULT 0 NULL,
	vpn int4 DEFAULT 0 NULL,
	class text,
	passed_challenge int4 DEFAULT 0 NULL,
	user_agent text NULL,
	country text NULL,
	continent text NULL,
	datacenter text NULL,
	hits int4 DEFAULT 0 NOT NULL,
	hit_rate int4 DEFAULT 0 NULL,
	num_user_agent int4 DEFAULT 1 NULL,
	duration float8 DEFAULT 0 NOT NULL,
	session_start timestamp NOT NULL,
	session_end timestamp NOT NULL,
	requests text NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT sessions_key PRIMARY KEY (session_id)
);
ALTER TABLE public.sessions ADD CONSTRAINT sessions_hostname_id_fkey FOREIGN KEY (hostname_id) REFERENCES public.hostname(hostname_id) ON DELETE CASCADE;
CREATE INDEX sessions_index ON sessions (session_end, host_name);

-- DROP TABLE public.challenge_command_history;

CREATE TABLE public.challenge_command_history (
	challenge_command_id uuid DEFAULT uuid_generate_v4() NOT NULL,
	hostname_id uuid NOT NULL,
	host_name text NOT NULL,
	command_type_name text DEFAULT ''::text NOT NULL,
	ip_address inet NOT NULL,
	session_cookie text DEFAULT ''::text NOT NULL,
	ip_cookie text NOT NULL,
	primary_session int4 DEFAULT 0 NULL,
	human int4 DEFAULT 0 NULL,
	passed_challenge int4 DEFAULT 0 NULL,
	user_agent text NULL,
	country text NULL,
	continent text NULL,
	datacenter text NULL,
	shapley text NULL,
	shapley_feature text NULL,
	difficulty int4 DEFAULT 0 NULL,
	hits int4 DEFAULT 0 NOT NULL,
	hit_rate int4 DEFAULT 0 NULL,
	num_user_agent int4 DEFAULT 1 NULL,
	session_start timestamp NOT NULL,
	session_end timestamp NOT NULL,
	requests text NULL,
	meta text DEFAULT ''::text NOT NULL,
	"source" text DEFAULT ''::text NOT NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_by text NOT NULL,
	duration float8 DEFAULT 0.0 NOT NULL,
	score float8 DEFAULT 0.0 NOT NULL,
	request_count int4 DEFAULT 0 NOT NULL,
	CONSTRAINT challenge_command_history_pkey PRIMARY KEY (challenge_command_id)
);
CREATE INDEX idx_hostname_command_type_to_command_history ON public.challenge_command_history USING btree (hostname_id, command_type_name);
CREATE INDEX commands_index ON challenge_command_history (session_end, host_name);



-- public.challenge_command_history foreign keys

ALTER TABLE public.challenge_command_history ADD CONSTRAINT challenge_command_history_hostname_id_fkey FOREIGN KEY (hostname_id) REFERENCES public.hostname(hostname_id) ON DELETE CASCADE;

